from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
import random


@dataclass
class Process:
    pid: int
    name: str
    arrival_time: int
    burst_time: int
    priority: int = 1
    state: str = "NEW"
    remaining_time: int = 0
    waiting_time: int = 0
    turnaround_time: int = 0
    response_time: Optional[int] = None
    completion_time: Optional[int] = None
    parent_pid: Optional[int] = None
    children: List[int] = field(default_factory=list)
    memory_block: int = 0
    io_burst: int = 0
    program: str = "default"

    def __post_init__(self):
        self.remaining_time = self.burst_time

    def fork(self, new_pid: int, name: str) -> "Process":
        child = Process(
            pid=new_pid,
            name=name,
            arrival_time=0,
            burst_time=max(1, self.burst_time // 2),
            priority=self.priority,
            parent_pid=self.pid,
            program=self.program,
        )
        self.children.append(child.pid)
        return child

    def exec(self, new_program: str):
        self.program = new_program
        self.state = "READY"

    def wait(self):
        self.state = "WAITING"

    def exit(self):
        self.state = "TERMINATED"

    def run(self, time_slice: int):
        if self.remaining_time <= 0:
            self.state = "TERMINATED"
            return 0
        quantum = min(time_slice, self.remaining_time)
        self.remaining_time -= quantum
        if self.remaining_time <= 0:
            self.state = "TERMINATED"
        else:
            self.state = "READY"
        return quantum


class Semaphore:
    def __init__(self, value: int = 1):
        self.value = value

    def wait(self):
        while self.value <= 0:
            pass
        self.value -= 1

    def signal(self):
        self.value += 1


class Pipe:
    def __init__(self):
        self.buffer: List[str] = []

    def write(self, message: str):
        self.buffer.append(message)

    def read(self):
        if not self.buffer:
            return None
        return self.buffer.pop(0)


class SharedMemory:
    def __init__(self):
        self.store: Dict[str, Any] = {}

    def write(self, key: str, value: Any):
        self.store[key] = value

    def read(self, key: str):
        return self.store.get(key)


class OSSimulator:
    def __init__(self):
        self.process_table: Dict[int, Process] = {}
        self.ready_queue: deque = deque()
        self.blocked_queue: List[Process] = []
        self.current_time = 0
        self.pipe_map: Dict[str, Pipe] = {}
        self.shared_memory = SharedMemory()
        self.semaphore_map: Dict[str, Semaphore] = {}
        self.event_log: List[str] = []

    def log(self, message: str):
        self.event_log.append(f"[{self.current_time}] {message}")

    def create_process(
        self,
        name: str,
        burst_time: int,
        priority: int = 1,
        arrival_time: int = 0,
        parent_pid: Optional[int] = None,
        program: str = "default",
    ) -> Process:
        pid = len(self.process_table) + 1
        p = Process(
            pid=pid,
            name=name,
            arrival_time=arrival_time,
            burst_time=burst_time,
            priority=priority,
            parent_pid=parent_pid,
            program=program,
        )
        p.state = "READY"
        self.process_table[pid] = p
        self.ready_queue.append(p)
        self.log(f"Process {pid} ({name}) created. Arrival={arrival_time}, Burst={burst_time}, Priority={priority}")
        return p

    def create_pipe(self) -> str:
        pipe_name = f"pipe_{len(self.pipe_map) + 1}"
        self.pipe_map[pipe_name] = Pipe()
        self.log(f"Pipe {pipe_name} created.")
        return pipe_name

    def create_shared_memory(self):
        self.log("Shared memory created.")
        return self.shared_memory

    def create_semaphore(self, name: str, value: int = 1):
        self.semaphore_map[name] = Semaphore(value)
        self.log(f"Semaphore {name} initialized with value {value}.")

    def fork_process(self, parent_pid: int, child_name: str) -> Process:
        parent = self.process_table[parent_pid]
        child_pid = len(self.process_table) + 1
        child = parent.fork(child_pid, child_name)
        child.arrival_time = self.current_time
        child.state = "READY"
        self.process_table[child_pid] = child
        self.ready_queue.append(child)
        self.log(f"fork(): Parent {parent_pid} created child {child_pid} ({child_name}).")
        return child

    def exec_process(self, pid: int, new_program: str):
        proc = self.process_table[pid]
        proc.exec(new_program)
        self.log(f"exec(): Process {pid} loaded program '{new_program}'.")

    def wait_process(self, pid: int):
        proc = self.process_table[pid]
        proc.wait()
        self.log(f"wait(): Process {pid} is waiting.")

    def exit_process(self, pid: int):
        proc = self.process_table[pid]
        proc.exit()
        self.log(f"exit(): Process {pid} terminated.")

    def pipe_write(self, pipe_name: str, message: str):
        pipe = self.pipe_map[pipe_name]
        pipe.write(message)
        self.log(f"Pipe {pipe_name} wrote: {message}")

    def pipe_read(self, pipe_name: str):
        pipe = self.pipe_map[pipe_name]
        msg = pipe.read()
        self.log(f"Pipe {pipe_name} read: {msg}")
        return msg

    def semaphore_wait(self, name: str):
        sem = self.semaphore_map[name]
        sem.wait()
        self.log(f"Semaphore {name} acquired.")

    def semaphore_signal(self, name: str):
        sem = self.semaphore_map[name]
        sem.signal()
        self.log(f"Semaphore {name} released.")

    def compute_metrics(self, processes: List[Process]) -> Dict[str, float]:
        avg_wait = sum(p.waiting_time for p in processes) / max(len(processes), 1)
        avg_turn = sum(p.turnaround_time for p in processes) / max(len(processes), 1)
        avg_response = sum(p.response_time or 0 for p in processes) / max(len(processes), 1)
        total_cpu = sum(p.burst_time for p in processes)
        total_time = max((p.completion_time or 0) for p in processes) if processes else 0
        cpu_util = (total_cpu / max(total_time, 1)) * 100 if total_time else 0.0
        throughput = len([p for p in processes if p.completion_time is not None]) / max(total_time, 1)

        return {
            "average_waiting_time": avg_wait,
            "average_turnaround_time": avg_turn,
            "average_response_time": avg_response,
            "cpu_utilization": cpu_util,
            "throughput": throughput,
        }

    def _schedule_process(self, processes: List[Process], algorithm: str, quantum: int = 2) -> List[Process]:
        if algorithm == "FCFS":
            ordered = sorted(processes, key=lambda p: (p.arrival_time, p.pid))
            current_time = 0
            for p in ordered:
                if p.arrival_time > current_time:
                    current_time = p.arrival_time
                if p.response_time is None:
                    p.response_time = max(0, current_time - p.arrival_time)
                current_time += p.burst_time
                p.completion_time = current_time
                p.turnaround_time = p.completion_time - p.arrival_time
                p.waiting_time = p.turnaround_time - p.burst_time
                self.log(f"FCFS schedule: {p.name} executes from {max(0, current_time - p.burst_time)} to {p.completion_time}")
            return ordered

        if algorithm == "SJF":
            current_time = 0
            remaining = [p for p in processes]
            completed: List[Process] = []
            while len(completed) < len(remaining):
                ready = [p for p in remaining if p.arrival_time <= current_time and p.completion_time is None]
                if not ready:
                    current_time = min(p.arrival_time for p in remaining if p.completion_time is None)
                    continue
                p = min(ready, key=lambda x: (x.burst_time, x.pid))
                if p.response_time is None:
                    p.response_time = max(0, current_time - p.arrival_time)
                current_time += p.burst_time
                p.completion_time = current_time
                p.turnaround_time = p.completion_time - p.arrival_time
                p.waiting_time = p.turnaround_time - p.burst_time
                completed.append(p)
                remaining.remove(p)
                self.log(f"SJF schedule: {p.name} executes until {p.completion_time}")
            return completed

        if algorithm == "Priority":
            current_time = 0
            remaining = [p for p in processes]
            completed: List[Process] = []
            while len(completed) < len(remaining):
                ready = [p for p in remaining if p.arrival_time <= current_time and p.completion_time is None]
                if not ready:
                    current_time = min(p.arrival_time for p in remaining if p.completion_time is None)
                    continue
                p = min(ready, key=lambda x: (x.priority, x.arrival_time, x.pid))
                if p.response_time is None:
                    p.response_time = max(0, current_time - p.arrival_time)
                current_time += p.burst_time
                p.completion_time = current_time
                p.turnaround_time = p.completion_time - p.arrival_time
                p.waiting_time = p.turnaround_time - p.burst_time
                completed.append(p)
                remaining.remove(p)
                self.log(f"Priority schedule: {p.name} executes until {p.completion_time}")
            return completed

        if algorithm == "Round Robin":
            ready = deque(sorted(processes, key=lambda p: (p.arrival_time, p.pid)))
            current_time = 0
            completed: List[Process] = []
            active: List[Process] = []
            while ready or active:
                while ready and ready[0].arrival_time <= current_time:
                    active.append(ready.popleft())
                if not active:
                    current_time = ready[0].arrival_time
                    continue
                p = active.pop(0)
                if p.response_time is None:
                    p.response_time = max(0, current_time - p.arrival_time)
                slice_time = min(quantum, p.remaining_time)
                current_time += slice_time
                p.remaining_time -= slice_time
                for proc in active:
                    if proc.state == "READY":
                        proc.waiting_time += slice_time
                if p.remaining_time > 0:
                    p.state = "READY"
                    ready.append(p)
                    self.log(f"RR schedule: {p.name} time slice used; requeued at time {current_time}")
                else:
                    p.completion_time = current_time
                    p.turnaround_time = p.completion_time - p.arrival_time
                    p.waiting_time = p.turnaround_time - p.burst_time
                    completed.append(p)
                    self.log(f"RR schedule: {p.name} completes at {p.completion_time}")
                # Add new arrivals that occur during this slice
                while ready and ready[0].arrival_time <= current_time:
                    active.append(ready.popleft())
                if active and ready and ready[0].arrival_time > current_time:
                    # prevent starvation in simple simulation
                    pass
            return completed

        raise ValueError(f"Unsupported algorithm: {algorithm}")

    def run_algorithm(self, algorithm: str, quantum: int = 2) -> Dict[str, Any]:
        processes = list(self.process_table.values())
        if not processes:
            raise ValueError("No processes to run.")

        scheduled = self._schedule_process(processes, algorithm, quantum)
        metrics = self.compute_metrics(scheduled)
        print(f"\n=== {algorithm} Scheduling Results ===")
        print(f"{'PID':<5} {'Process':<10} {'Arrival':<8} {'Burst':<6} {'Priority':<8} {'Wait':<7} {'Turn':<8} {'Resp':<8} {'Done':<8}")
        for p in sorted(scheduled, key=lambda x: x.pid):
            print(
                f"{p.pid:<5} {p.name:<10} {p.arrival_time:<8} {p.burst_time:<6} {p.priority:<8} "
                f"{p.waiting_time:<7} {p.turnaround_time:<8} {p.response_time if p.response_time is not None else 0:<8} {p.completion_time if p.completion_time is not None else 0:<8}"
            )

        print(f"\nAverage Waiting Time: {metrics['average_waiting_time']:.2f}")
        print(f"Average Turnaround Time: {metrics['average_turnaround_time']:.2f}")
        print(f"Average Response Time: {metrics['average_response_time']:.2f}")
        print(f"CPU Utilization: {metrics['cpu_utilization']:.2f}%")
        print(f"Throughput: {metrics['throughput']:.3f} procs/unit time")

        return {
            "scheduled": scheduled,
            "metrics": metrics,
        }


def demo():
    os_sim = OSSimulator()

    # Operating system role demonstration
    os_sim.create_semaphore("printer", 1)
    os_sim.create_semaphore("shared_buffer", 1)

    # Create example processes
    p1 = os_sim.create_process("P1", burst_time=8, priority=3, arrival_time=0, program="cpu_task")
    p2 = os_sim.create_process("P2", burst_time=4, priority=1, arrival_time=1, program="memory_task")
    p3 = os_sim.create_process("P3", burst_time=9, priority=2, arrival_time=2, program="io_task")
    p4 = os_sim.create_process("P4", burst_time=5, priority=4, arrival_time=3, program="scheduler_task")

    # Demonstrate fork() and exec()
    child1 = os_sim.fork_process(p1.pid, "P1_child")
    os_sim.exec_process(child1.pid, "grep")

    # Demonstrate IPC using pipe and shared memory
    pipe_name = os_sim.create_pipe()
    os_sim.pipe_write(pipe_name, "Hello from parent process")
    msg = os_sim.pipe_read(pipe_name)
    os_sim.shared_memory.write("counter", 10)
    os_sim.shared_memory.write("counter", os_sim.shared_memory.read("counter") + 5)

    # Demonstrate semaphore synchronization
    os_sim.semaphore_wait("printer")
    os_sim.semaphore_signal("printer")

    # Run scheduling algorithms
    algorithms = ["FCFS", "SJF", "Priority", "Round Robin"]
    results = {}
    for algo in algorithms:
        if algo == "Round Robin":
            results[algo] = os_sim.run_algorithm(algo, quantum=2)
        else:
            results[algo] = os_sim.run_algorithm(algo)

    print("\n=== Event Log ===")
    for event in os_sim.event_log:
        print(event)

    print("\n=== System Overview ===")
    print("The OS manages process creation, scheduling, IPC, synchronization, and resource control.")
    print("fork() creates child processes, exec() loads programs, pipes implement communication, and semaphores protect shared resources.")
    print("Scheduling algorithms like FCFS, SJF, Priority, and Round Robin are compared by waiting time, turnaround time, and CPU utilization.")


if __name__ == "__main__":
    demo()
    # Example to run custom algorithm only:
    # os_sim = OSSimulator()
    # ... create processes ...
    # os_sim.run_algorithm("Round Robin", quantum=2)
