#!/usr/bin/env python3
"""
Infrastructure Log Generator
Generates unstructured syslog-style log lines at ~300 events/sec.
Writes to /var/log/generators/infra-{hostname}.log (one file per host).
~10% of lines contain error/warn keywords.
"""

import os
import random
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = "/var/log/generators"
TARGET_RATE = 300          # events per second (across all hosts)
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB

HOSTNAMES = [f"k8s-node-{i:02d}" for i in range(1, 11)]

PROCESSES = [
    "systemd", "sshd", "cron", "kernel", "dockerd",
    "containerd", "kubelet", "nginx", "postgres", "redis",
]

# PID ranges per process (for realism)
_PID_RANGES = {
    "systemd":    (1, 1),
    "sshd":       (800, 1200),
    "cron":       (1300, 1400),
    "kernel":     (0, 0),
    "dockerd":    (1500, 1600),
    "containerd": (1600, 1700),
    "kubelet":    (1700, 1900),
    "nginx":      (2000, 2200),
    "postgres":   (2200, 2400),
    "redis":      (2400, 2500),
}

# Normal messages per process
NORMAL_MESSAGES = {
    "systemd": [
        "Started Session {sid} of user ubuntu.",
        "Stopping Session {sid} of user ubuntu.",
        "Started Daily apt upgrade and clean activities.",
        "Reached target Multi-User System.",
        "Reloading The nginx HTTP Server.",
        "Started Kubernetes kubelet.",
        "Finished Rotate log files.",
        "Started Docker Application Container Engine.",
    ],
    "sshd": [
        "Accepted publickey for ubuntu from {ip} port {port} ssh2",
        "Disconnected from user ubuntu {ip} port {port}",
        "pam_unix(sshd:session): session opened for user ubuntu by (uid=0)",
        "pam_unix(sshd:session): session closed for user ubuntu",
        "Server listening on 0.0.0.0 port 22.",
        "Server listening on :: port 22.",
    ],
    "cron": [
        "(ubuntu) CMD (   cd / && run-parts --report /etc/cron.hourly)",
        "(root) CMD (/usr/lib/update-notifier/apt-check --human-readable)",
        "(CRON) INFO (No MTA installed, discarding output)",
        "pam_unix(cron:session): session opened for user root by (uid=0)",
        "pam_unix(cron:session): session closed for user root",
    ],
    "kernel": [
        "EXT4-fs (sda1): re-mounted. Opts: errors=remount-ro",
        "NET: Registered PF_INET6 protocol family",
        "audit: type=1400 audit({ts}:42): apparmor='ALLOWED' operation='exec'",
        "perf: interrupt took too long ({ms} > {ms2}), lowering kernel.perf_event_max_sample_rate",
        "IPv6: ADDRCONF(NETDEV_CHANGE): eth0: link becomes ready",
        "oom_kill_process: Kill process {pid} score {score} or sacrifice child",
    ],
    "dockerd": [
        "time=\"{ts}\" level=info msg=\"ignoring event\" module=libcontainerd namespace=moby topic=/tasks/delete",
        "time=\"{ts}\" level=info msg=\"Container started\" containerId={cid}",
        "time=\"{ts}\" level=info msg=\"Handler for DELETE /containers/{cid} returned with code 204\"",
        "time=\"{ts}\" level=info msg=\"NetworkController.cleanup: successfully released all resources\"",
        "time=\"{ts}\" level=info msg=\"shim disconnected\" id={cid}",
    ],
    "containerd": [
        "time=\"{ts}\" level=info msg=\"starting containerd\" revision=1.6.8",
        "time=\"{ts}\" level=info msg=\"loading plugin\" type=io.containerd.snapshotter.v1 id=overlayfs",
        "time=\"{ts}\" level=info msg=\"serving...\" address=/run/containerd/containerd.sock",
        "time=\"{ts}\" level=info msg=\"Start subscribing containerd event\"",
        "time=\"{ts}\" level=info msg=\"TaskDelete event\" containerID={cid}",
    ],
    "kubelet": [
        "I0115 {hms} kubelet.go:2163] Starting kubelet on node {node}",
        "I0115 {hms} server.go:407] Version: v1.28.4",
        "I0115 {hms} pod_workers.go:1281] Sync pod completed successfully node={node} pod={pod}",
        "I0115 {hms} volume_manager.go:220] Starting Kubelet Volume Manager",
        "I0115 {hms} status_manager.go:165] Starting to sync pod statuses with apiserver",
        "I0115 {hms} eviction_manager.go:333] eviction manager: no observation provided for resource memory.available",
    ],
    "nginx": [
        "2024/01/15 {hms} [notice] {pid}#{pid2}: signal process started",
        "2024/01/15 {hms} [notice] {pid}#{pid2}: gracefully shutting down",
        "2024/01/15 {hms} [notice] {pid}#{pid2}: worker process exited with code 0",
        "2024/01/15 {hms} [info] {pid}#{pid2}: *{reqid} client closed connection while waiting for request",
        "2024/01/15 {hms} [notice] {pid}#{pid2}: start worker process {wid}",
    ],
    "postgres": [
        "LOG:  database system was shut down at 2024-01-15 {hms} UTC",
        "LOG:  entering standby mode",
        "LOG:  redo starts at 0/{lsn}",
        "LOG:  consistent recovery state reached at 0/{lsn}",
        "LOG:  database system is ready to accept read only connections",
        "LOG:  checkpoint starting: time",
        "LOG:  checkpoint complete: wrote {qty} buffers ({pct}%); 0 WAL file(s) added",
        "LOG:  autovacuum: processing database \"{db}\"",
        "LOG:  duration: {ms}.000 ms  statement: SELECT 1",
    ],
    "redis": [
        "* Ready to accept connections",
        "* Background saving started by pid {pid}",
        "* DB saved on disk",
        "* Background saving terminated with success",
        "# Server started, Redis version=7.2.3",
        "* Replica {ip}:{port} asks for synchronization",
        "* Partial resynchronization request from {ip}:{port} accepted",
        "* Synchronization with replica {ip}:{port} succeeded",
    ],
}

# Error/warn messages (~10% of lines)
ERROR_MESSAGES = {
    "systemd": [
        "Failed to start Session {sid} of user ubuntu: timeout",
        "Unit docker.service entered failed state.",
        "Failed to mount /mnt/data: error mounting: failed to mount",
    ],
    "sshd": [
        "error: Could not load host key: /etc/ssh/ssh_host_ecdsa_key",
        "Failed password for invalid user admin from {ip} port {port} ssh2",
        "Connection timed out from {ip} port {port}",
        "warning: /etc/hosts.allow, line 42: can't verify hostname: getaddrinfo",
    ],
    "cron": [
        "error: couldn't open log file /var/log/cron.log: Permission denied",
        "(CRON) error (grandchild #1247 failed with exit status 1)",
    ],
    "kernel": [
        "EXT4-fs error (device sda1): ext4_journal_check_start:61: Detected aborted journal",
        "NVRM: failed to copy vbios to system memory.",
        "WARNING: CPU: 0 PID: 1 at kernel/sched/core.c:3151",
        "oom-kill: constraint=CONSTRAINT_NONE, nodemask=(null), task=python3, pid={pid}",
    ],
    "dockerd": [
        "time=\"{ts}\" level=error msg=\"Handler for POST /commit returned error: no such container\"",
        "time=\"{ts}\" level=warning msg=\"failed to retrieve docker-init version\"",
        "time=\"{ts}\" level=error msg=\"containerd: deleting container\" error=\"context deadline exceeded\"",
    ],
    "containerd": [
        "time=\"{ts}\" level=error msg=\"failed to handle event\" error=\"context canceled\"",
        "time=\"{ts}\" level=warning msg=\"cleanup warnings\" warning=\"overlay: failed to remove layer\"",
    ],
    "kubelet": [
        "E0115 {hms} pod_workers.go:951] Error syncing pod failed error=\"timeout waiting for volumes to attach\"",
        "W0115 {hms} reflector.go:535] k8s.io/client-go/informers/factory.go:150: watch of *v1.Node exceeded request timeout",
        "E0115 {hms} kubelet.go:2187] node problem: failed to get node info: timeout",
        "W0115 {hms} volume_manager.go:220] Timeout expired waiting for volumes to attach",
    ],
    "nginx": [
        "2024/01/15 {hms} [error] {pid}#{pid2}: *{reqid} connect() failed (111: Connection refused) upstream",
        "2024/01/15 {hms} [warn] {pid}#{pid2}: *{reqid} upstream server temporarily disabled",
        "2024/01/15 {hms} [error] {pid}#{pid2}: *{reqid} SSL_do_handshake() failed",
    ],
    "postgres": [
        "ERROR:  deadlock detected",
        "FATAL:  remaining connection slots are reserved for non-replication superuser connections",
        "WARNING:  out of shared memory",
        "ERROR:  could not serialize access due to concurrent update",
        "LOG:  connection timeout: client failed to connect within {ms}ms",
    ],
    "redis": [
        "# WARNING: 32 bit instance detected but no memory limit set.",
        "# Can't save in background: fork: Cannot allocate memory",
        "# Failed opening .rdb for saving: Permission denied",
        "* FAIL message received from 9e5a2b... about e21f7a...",
        "# WARNING: no config file specified, using the default config",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def syslog_timestamp() -> str:
    dt = datetime.now(timezone.utc)
    return f"{MONTHS[dt.month - 1]} {dt.day:2d} {dt.strftime('%H:%M:%S')}"


def fill(template: str, proc: str, hostname: str) -> str:
    pid_lo, pid_hi = _PID_RANGES.get(proc, (1000, 9999))
    pid = pid_lo if pid_lo == pid_hi else random.randint(pid_lo, pid_hi)
    dt = datetime.now(timezone.utc)
    hms = dt.strftime("%H:%M:%S")
    ts = dt.strftime("%Y-%m-%dT%H:%M:%S")
    return (
        template
        .replace("{sid}", str(random.randint(1, 999)))
        .replace("{ip}", f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}")
        .replace("{port}", str(random.randint(49152, 65535)))
        .replace("{ts}", ts)
        .replace("{hms}", hms)
        .replace("{ms}", str(random.randint(10, 9999)))
        .replace("{ms2}", str(random.randint(10, 9999)))
        .replace("{qty}", str(random.randint(1, 9999)))
        .replace("{pct}", f"{random.uniform(0.1, 99.9):.1f}")
        .replace("{pid}", str(pid))
        .replace("{pid2}", str(random.randint(1, 99)))
        .replace("{wid}", str(random.randint(10000, 99999)))
        .replace("{reqid}", str(random.randint(1, 999999)))
        .replace("{lsn}", f"{random.randint(100000, 999999):X}")
        .replace("{db}", random.choice(["orders", "users", "inventory", "payments"]))
        .replace("{cid}", "".join(random.choices("0123456789abcdef", k=12)))
        .replace("{node}", hostname)
        .replace("{pod}", f"pod-{random.randint(1000, 9999)}")
        .replace("{score}", str(random.randint(100, 1000)))
    )


def format_line(timestamp: str, hostname: str, proc: str, pid: int, message: str) -> str:
    if proc == "kernel":
        return f"{timestamp} {hostname} {proc}: {message}"
    return f"{timestamp} {hostname} {proc}[{pid}]: {message}"


class RotatingFile:
    def __init__(self, path: str, max_size: int):
        self.path = path
        self.max_size = max_size
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._f = open(path, "a", buffering=8192)
        self._f.seek(0, 2)
        self._size = self._f.tell()

    def write(self, line: str):
        encoded = (line + "\n").encode("utf-8")
        if self._size + len(encoded) > self.max_size:
            self._rotate()
        self._f.write(line + "\n")
        self._size += len(encoded)

    def _rotate(self):
        self._f.close()
        rotated = self.path + ".1"
        if os.path.exists(rotated):
            os.remove(rotated)
        os.rename(self.path, rotated)
        self._f = open(self.path, "a", buffering=8192)
        self._size = 0

    def flush(self):
        self._f.flush()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = {
        host: RotatingFile(
            os.path.join(OUTPUT_DIR, f"infra-{host}.log"),
            MAX_FILE_SIZE,
        )
        for host in HOSTNAMES
    }

    flush_every = 300
    count = 0

    print(f"Infra log generator started. Target rate: {TARGET_RATE} events/sec", flush=True)

    while True:
        start = time.monotonic()

        for _ in range(TARGET_RATE):
            host = random.choice(HOSTNAMES)
            proc = random.choice(PROCESSES)
            pid_lo, pid_hi = _PID_RANGES.get(proc, (1000, 9999))
            pid = pid_lo if pid_lo == pid_hi else random.randint(pid_lo, pid_hi)

            # 10% chance of error/warn message
            use_error = random.random() < 0.10
            if use_error and ERROR_MESSAGES.get(proc):
                template = random.choice(ERROR_MESSAGES[proc])
            else:
                template = random.choice(NORMAL_MESSAGES[proc])

            message = fill(template, proc, host)
            ts = syslog_timestamp()
            line = format_line(ts, host, proc, pid, message)
            files[host].write(line)

            count += 1
            if count % flush_every == 0:
                for f in files.values():
                    f.flush()

        elapsed = time.monotonic() - start
        sleep_for = 1.0 - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
