/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "resource.hh"
#include "core/align.hh"

namespace resource {

size_t calculate_memory(configuration c, size_t available_memory) {
    auto default_reserve_memory = std::max<size_t>(1 << 30, 0.05 * available_memory);
    available_memory -= c.reserve_memory.value_or(default_reserve_memory);
    size_t mem = c.total_memory.value_or(available_memory);
    if (mem > available_memory) {
        throw std::runtime_error("insufficient physical memory");
    }
    return mem;
}

}

#ifdef HAVE_HWLOC

#include "util/defer.hh"
#include "core/print.hh"
#include <hwloc.h>
#include <unordered_map>

cpu_set_t cpuid_to_cpuset(unsigned cpuid) {
    cpu_set_t cs;
    CPU_ZERO(&cs);
    CPU_SET(cpuid, &cs);
    return cs;
}

namespace resource {

size_t div_roundup(size_t num, size_t denom) {
    return (num + denom - 1) / denom;
}

static unsigned find_memory_depth(hwloc_topology_t& topology) {
    auto depth = hwloc_get_type_depth(topology, HWLOC_OBJ_PU);
    auto obj = hwloc_get_next_obj_by_depth(topology, depth, nullptr);

    while (!obj->memory.local_memory && obj) {
        obj = hwloc_get_ancestor_obj_by_depth(topology, --depth, obj);
    }
    assert(obj);
    return depth;
}

static size_t alloc_from_node(cpu& this_cpu, hwloc_obj_t node, std::unordered_map<hwloc_obj_t, size_t>& used_mem, size_t alloc) {
    auto taken = std::min(node->memory.local_memory - used_mem[node], alloc);
    if (taken) {
        used_mem[node] += taken;
        auto node_id = hwloc_bitmap_first(node->nodeset);
        assert(node_id != -1);
        this_cpu.mem.push_back({taken, unsigned(node_id)});
    }
    return taken;
}

std::vector<cpu> allocate(configuration c) {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    auto free_hwloc = defer([&] { hwloc_topology_destroy(topology); });
    hwloc_topology_load(topology);
    if (c.cpu_set) {
        auto bm = hwloc_bitmap_alloc();
        auto free_bm = defer([&] { hwloc_bitmap_free(bm); });
        for (auto idx : *c.cpu_set) {
            hwloc_bitmap_set(bm, idx);
        }
        auto r = hwloc_topology_restrict(topology, bm,
                HWLOC_RESTRICT_FLAG_ADAPT_DISTANCES
                | HWLOC_RESTRICT_FLAG_ADAPT_MISC
                | HWLOC_RESTRICT_FLAG_ADAPT_IO);
        if (r == -1) {
            if (errno == ENOMEM) {
                throw std::bad_alloc();
            }
            if (errno == EINVAL) {
                throw std::runtime_error("bad cpuset");
            }
            abort();
        }
    }
    auto machine_depth = hwloc_get_type_depth(topology, HWLOC_OBJ_MACHINE);
    assert(hwloc_get_nbobjs_by_depth(topology, machine_depth) == 1);
    auto machine = hwloc_get_obj_by_depth(topology, machine_depth, 0);
    auto available_memory = machine->memory.total_memory;
    size_t mem = calculate_memory(c, available_memory);
    unsigned available_procs = hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
    unsigned procs = c.cpus.value_or(available_procs);
    if (procs > available_procs) {
        throw std::runtime_error("insufficient processing units");
    }
    auto mem_per_proc = align_down<size_t>(mem / procs, 2 << 20);
    std::vector<hwloc_cpuset_t> cpu_sets{procs};
    auto free_cpu_sets = defer([&] {
        for (auto&& cs : cpu_sets) {
            hwloc_bitmap_free(cs);
        }
    });
    auto root = hwloc_get_root_obj(topology);
#if HWLOC_API_VERSION >= 0x00010900
    hwloc_distrib(topology, &root, 1, cpu_sets.data(), cpu_sets.size(), INT_MAX, 0);
#else
    hwloc_distribute(topology, root, cpu_sets.data(), cpu_sets.size(), INT_MAX);
#endif
    std::vector<cpu> ret;
    std::unordered_map<hwloc_obj_t, size_t> topo_used_mem;
    std::vector<std::pair<cpu, size_t>> remains;
    size_t remain;
    unsigned depth = find_memory_depth(topology);

    // Divide local memory to cpus
    for (auto&& cs : cpu_sets) {
        auto cpu_id = hwloc_bitmap_first(cs);
        assert(cpu_id != -1);
        auto pu = hwloc_get_pu_obj_by_os_index(topology, cpu_id);
        auto node = hwloc_get_ancestor_obj_by_depth(topology, depth, pu); 
        cpu this_cpu;
        this_cpu.cpu_id = cpu_id;
        remain = mem_per_proc - alloc_from_node(this_cpu, node, topo_used_mem, mem_per_proc);

        remains.emplace_back(std::move(this_cpu), remain); 
    }

    // Divide the rest of the memory
    for (auto&& r : remains) {
        cpu this_cpu;
        size_t remain; 
        std::tie(this_cpu, remain) = r;
        auto pu = hwloc_get_pu_obj_by_os_index(topology, this_cpu.cpu_id);
        auto node = hwloc_get_ancestor_obj_by_depth(topology, depth, pu); 
        auto obj = node;

        while (remain) {
            remain -= alloc_from_node(this_cpu, obj, topo_used_mem, remain);
            do {
                obj = hwloc_get_next_obj_by_depth(topology, depth, obj);
            } while (!obj);
            if (obj == node)
                break;
        }
        assert(!remain);
        ret.push_back(std::move(this_cpu));
    }
    return ret;
}

io_queue_topology allocate_io_queues(configuration c, std::vector<cpu> cpus) {
    unsigned num_io_queues;
    unsigned max_io_requests;
    if (c.io_queues) {
        num_io_queues = *c.io_queues;
    } else {
        num_io_queues = cpus.size();
    }
    if (c.max_io_requests) {
        max_io_requests = *c.max_io_requests;
    } else {
        max_io_requests = 128 * num_io_queues;
    }

    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    auto free_hwloc = defer([&] { hwloc_topology_destroy(topology); });
    hwloc_topology_load(topology);
    unsigned depth = find_memory_depth(topology);

    // Not an unordered_multimap, so we can preserver ordering within mapped values
    std::unordered_map<unsigned, std::vector<unsigned>> numa_nodes;
    auto shard = int(cpus.size());
    while (--shard >= 0) {
        auto pu = hwloc_get_pu_obj_by_os_index(topology, cpus[shard].cpu_id);
        auto node = hwloc_get_ancestor_obj_by_depth(topology, depth, pu);
        auto node_id = hwloc_bitmap_first(node->nodeset);

        if (numa_nodes.count(node_id) == 0) {
            numa_nodes.emplace(node_id, std::vector<unsigned>());
        }
        numa_nodes.at(node_id).push_back(shard);
    }

    io_queue_topology ret;

    ret.shard_to_coordinator.resize(cpus.size());
    ret.coordinators.resize(num_io_queues);

    // If we have more than one node, we will mandate at least one coordinator
    // per node. It simplifies the coordinator assignment and in real scenarios
    // we don't want to be passing things around to the other side of the box
    // anyway. We could silently adjust, but it is better to avoid surprises.
    if ((num_io_queues < numa_nodes.size()) || (num_io_queues > cpus.size())) {
        auto msg = sprint("Invalid number of IO queues. Asked for %d. Minimum value is %d, maximum %d", num_io_queues, numa_nodes.size(), cpus.size());
        throw std::runtime_error(std::move(msg));
    }

    // First step is to distribute the io coordinators among the NUMA nodes as
    // equally as we can. We will do that by round robin assignment to the nodes.
    unsigned node = 0;
    std::unordered_map<unsigned, std::vector<unsigned>> node_coordinators;
    for (auto cid = 0u; cid < num_io_queues; ++cid) {
        unsigned node_id;
        // This won't loop forever because we have already established that there are less
        // io queues than processors.
        do {
            node_id = node++ % numa_nodes.size();
        } while (numa_nodes[node_id].empty());
        auto& curr_node = numa_nodes[node_id];

        auto io_coordinator = curr_node.back();
        curr_node.pop_back();

        ret.shard_to_coordinator[io_coordinator] = io_coordinator;
        ret.coordinators[cid].capacity =  std::max(max_io_requests / num_io_queues , 1u);
        ret.coordinators[cid].id = io_coordinator;

        if (node_coordinators.count(node_id) == 0) {
            node_coordinators.emplace(node_id, std::vector<unsigned>());
        }
        node_coordinators.at(node_id).push_back(io_coordinator);
    }

    // If there are more processors than coordinators, we will have to assign them to existing
    // coordinators. We always do that within the same NUMA node.
    for (auto& node: numa_nodes) {
        auto cid_idx = 0;
        for (auto& remaining_shard: node.second) {
            auto idx = cid_idx++ % node_coordinators.at(node.first).size();
            auto io_coordinator = node_coordinators.at(node.first)[idx];
            ret.shard_to_coordinator[remaining_shard] = io_coordinator;
        }
    }

    return ret;
}

unsigned nr_processing_units() {
    hwloc_topology_t topology;
    hwloc_topology_init(&topology);
    auto free_hwloc = defer([&] { hwloc_topology_destroy(topology); });
    hwloc_topology_load(topology);
    return hwloc_get_nbobjs_by_type(topology, HWLOC_OBJ_PU);
}

}

#else

#include "resource.hh"
#include <unistd.h>

namespace resource {

std::vector<cpu> allocate(configuration c) {
    auto available_memory = ::sysconf(_SC_PAGESIZE) * size_t(::sysconf(_SC_PHYS_PAGES));
    auto mem = calculate_memory(c, available_memory);
    auto cpuset_procs = c.cpu_set ? c.cpu_set->size() : nr_processing_units();
    auto procs = c.cpus.value_or(cpuset_procs);
    std::vector<cpu> ret;
    ret.reserve(procs);
    for (unsigned i = 0; i < procs; ++i) {
        ret.push_back(cpu{i, {{mem / procs, 0}}});
    }
    return ret;
}

// Without hwloc, we don't support tuning the number of IO queues. So each CPU gets their.
io_queue_topology allocate_io_queues(configuration c, std::vector<cpu> cpus) {
    io_queue_topology ret;

    unsigned max_io_requests;
    unsigned nr_cpus = unsigned(cpus.size());
    if (c.max_io_requests) {
        max_io_requests = *c.max_io_requests;
    } else {
        max_io_requests = 128 * nr_cpus;
    }

    ret.shard_to_coordinator.resize(nr_cpus);
    ret.coordinators.resize(nr_cpus);

    for (unsigned shard = 0; shard < nr_cpus; ++shard) {
        ret.shard_to_coordinator[shard] = shard;
        ret.coordinators[shard].capacity =  std::max(max_io_requests / nr_cpus, 1u);
        ret.coordinators[shard].id = shard;
    }
    return ret;
}

unsigned nr_processing_units() {
    return ::sysconf(_SC_NPROCESSORS_ONLN);
}

}

#endif
