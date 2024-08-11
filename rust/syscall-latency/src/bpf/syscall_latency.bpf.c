// SPDX-License-Identifier: GPL-2.0
// Copyright (c) 2019 Facebook
#include "vmlinux.h"
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_helpers.h>
//#include <linux/sched.h>
char LICENSE[] SEC("license") = "Dual BSD/GPL";

const volatile uint32_t MY_PID = 0;
const volatile uint32_t TARGET_PIDS[4] = { 0, 0, 0, 0 };

// from: /sys/kernel/debug/tracing/events/raw_syscalls/sys_enter/format
struct sys_enter_ctx {
	uint64_t pad;
	int64_t syscall_number;
	uint32_t args[6];
};

struct sys_exit_ctx {
	uint64_t pad;
	int64_t syscall_number;
	uint64_t ret;
};

struct syscall_event {
	uint32_t pid;
	uint32_t tid;
	uint64_t syscall_number;
	uint64_t timestamp;
};

struct syscall_buffer {
	uint32_t length;
	struct syscall_event buffer[256];
};

// PERF_EVENT_ARRAY to communicate with userspace
struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(key_size, sizeof(int));
	__uint(value_size, sizeof(int));
} perf_array_syscall_enter SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(key_size, sizeof(int));
	__uint(value_size, sizeof(int));
} perf_array_syscall_exit SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__type(key, __u32);
	__type(value, struct syscall_buffer);
	__uint(max_entries, 1);
} syscall_enter_buffer_map SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__type(key, __u32);
	__type(value, struct syscall_buffer);
	__uint(max_entries, 1);
} syscall_exit_buffer_map SEC(".maps");

bool qualifies(uint32_t pid, int syscall_number) {
	bool is_pid = false;
	for (int i = 0; i < 4; ++i) {
		int target_pid = TARGET_PIDS[i];
		if ((target_pid > 0) && (pid == target_pid)) {
			return true;
		}
	}
	return false;
}

SEC("tp/raw_syscalls/sys_exit")
int handle_sys_exit(struct sys_exit_ctx *ctx) {
    struct task_struct* task = (struct task_struct*)bpf_get_current_task();
    uint32_t pid = 0;
    uint32_t tid = 0;
    bpf_probe_read(&pid, sizeof(pid), &task->tgid);
    bpf_probe_read(&tid, sizeof(pid), &task->pid);
    int syscall_number = ctx->syscall_number;

	if (!qualifies(pid, syscall_number)) {
		return 0;
	}

    int zero = 0;
    uint64_t time = bpf_ktime_get_ns();

    struct syscall_event e = {0};
    e.pid = pid;
    e.tid = tid;
    e.syscall_number = syscall_number;
    e.timestamp = time;

    struct syscall_buffer *buffer = bpf_map_lookup_elem(&syscall_exit_buffer_map, &zero);
    if (!buffer) {
        bpf_printk("ERROR GETTING BUFFER");
        return 0;
    }

    if (buffer->length < 256) {
        buffer->buffer[buffer->length] = e;
        buffer->length += 1;
    }

    if (buffer->length == 256) {
        bpf_perf_event_output((void *)ctx, &perf_array_syscall_exit, BPF_F_CURRENT_CPU, buffer, sizeof(*buffer));
        buffer->length = 0;
    }

    return 0;
}

SEC("tp/raw_syscalls/sys_enter")
int handle_sys_enter(struct sys_enter_ctx *ctx) {
    struct task_struct* task = (struct task_struct*)bpf_get_current_task();
    uint32_t pid = 0;
    uint32_t tid = 0;
    bpf_probe_read(&pid, sizeof(pid), &task->tgid);
    bpf_probe_read(&tid, sizeof(pid), &task->pid);
    int syscall_number = ctx->syscall_number;

	if (!qualifies(pid, syscall_number)) {
		return 0;
	}

	//const char fmt_str[] = "My PID is %d, syscall: %d\n";
	//bpf_trace_printk(fmt_str, sizeof(fmt_str), pid, syscall_number);

    int zero = 0;
    uint64_t time = bpf_ktime_get_ns();

    struct syscall_event e = {0};
    e.pid = pid;
    e.tid = tid;
    e.syscall_number = syscall_number;
    e.timestamp = time;

    struct syscall_buffer *buffer = bpf_map_lookup_elem(&syscall_enter_buffer_map, &zero);
    if (!buffer) {
        bpf_printk("ERROR GETTING BUFFER");
        return 0;
    }

    if (buffer->length < 256) {
        buffer->buffer[buffer->length] = e;
        buffer->length += 1;
    }

    if (buffer->length == 256) {
        bpf_perf_event_output((void *)ctx, &perf_array_syscall_enter, BPF_F_CURRENT_CPU, buffer, sizeof(*buffer));
        buffer->length = 0;
    }

    return 0;
}
