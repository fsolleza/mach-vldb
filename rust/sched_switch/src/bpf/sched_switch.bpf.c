#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_tracing.h>

struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(key_size, sizeof(int));
	__uint(value_size, sizeof(int));
} pb SEC(".maps");

struct event {
    uint32_t prev_pid;
    uint32_t next_pid;
	uint64_t timestamp;
};

const volatile uint32_t target_pid = 0;

static int handle_switch(void *ctx, struct task_struct *prev, struct task_struct *next) {
    uint32_t prev_pid = 0;
    bpf_probe_read(&prev_pid, sizeof(prev_pid), &prev->tgid);

    if (prev_pid == target_pid) {
        uint32_t next_pid = 0;
        bpf_probe_read(&next_pid, sizeof(next_pid), &next->tgid);
        struct event e = {};
        e.prev_pid = prev_pid;
        e.next_pid = prev_pid;
        e.timestamp = bpf_ktime_get_ns();
        bpf_perf_event_output(ctx, &pb, BPF_F_CURRENT_CPU, &e, sizeof(e));
    }
}

SEC("raw_tp/sched_switch")
int BPF_PROG(handle_sched_switch, bool preempt, struct task_struct *prev, struct task_struct *next)
{
	(void)preempt;
    handle_switch(ctx, prev, next);

	return 0;
}

char LICENSE[] SEC("license") = "GPL";
