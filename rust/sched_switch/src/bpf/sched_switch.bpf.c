#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_core_read.h>
#include <bpf/bpf_tracing.h>

struct event {
    uint64_t prev_pid;
    uint64_t next_pid;
	uint64_t cpu;
	uint64_t timestamp;
    char comm[16];
};

struct {
	__uint(type, BPF_MAP_TYPE_PERF_EVENT_ARRAY);
	__uint(key_size, sizeof(int));
	__uint(value_size, sizeof(int));
} pb SEC(".maps");

//const volatile uint32_t target_pid1 = 0;
//const volatile uint32_t target_pid2 = 0;
const volatile uint32_t this_pid = 0;

#define TASK_COMM_LEN 16

static int handle_switch(void *ctx, struct task_struct *prev, struct task_struct *next) {
    uint32_t prev_pid = BPF_CORE_READ(prev, pid);
    //bpf_probe_read(&prev_pid, sizeof(prev_pid), &prev->tgid);

    uint32_t next_pid = BPF_CORE_READ(next, pid);
    //bpf_probe_read(&next_pid, sizeof(next_pid), &next->tgid);

    struct event e = {0};
    e.prev_pid = prev_pid;
    e.next_pid = next_pid;
    e.timestamp = bpf_ktime_get_ns();
    e.cpu = bpf_get_smp_processor_id();

    //bool in_cpu = e.cpu == 1 || e.cpu == 3 || e.cpu == 5;
    bool in_cpu = e.cpu >= 1 && e.cpu <= 9;
    bool not_this = e.prev_pid != this_pid && e.next_pid != this_pid;

    if (in_cpu && not_this) {
        bpf_probe_read_kernel_str(&e.comm, sizeof(e.comm), prev->comm);
        bpf_printk("Switching in %s, %d, %d", e.comm, e.cpu, prev_pid);
        bpf_perf_event_output(ctx, &pb, BPF_F_CURRENT_CPU, &e, sizeof(e));
    }
    //}

    //if (next_pid != prev_pid &&
    //    (prev_pid == target_pid1 || prev_pid == target_pid2)) {
    //}
}

SEC("tp_btf/sched_switch")
int BPF_PROG(sched_switch, bool preempt, struct task_struct *prev, struct task_struct *next)
{
	(void)preempt;
    handle_switch(ctx, prev, next);

	return 0;
}

SEC("raw_tp/sched_switch")
int BPF_PROG(handle_sched_switch, bool preempt, struct task_struct *prev, struct task_struct *next)
{
	(void)preempt;
    handle_switch(ctx, prev, next);

	return 0;
}

char LICENSE[] SEC("license") = "GPL";
