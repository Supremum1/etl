package bench;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

final class ProcessMetrics {
    private ProcessMetrics() {
    }

    static double elapsedMs(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000.0;
    }

    static long cpuMs() {
        var bean = ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class);
        long nanos = bean == null ? -1 : bean.getProcessCpuTime();
        return nanos >= 0 ? nanos / 1_000_000 : 0;
    }

    static double rssMb() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.replaceAll("[^0-9]", "")) / 1024.0;
                }
            }
        } catch (Exception ignored) {
            // /proc is available in Docker; the heap fallback is for local runs.
        }
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / 1024.0 / 1024.0;
    }

    static final class MemorySampler implements AutoCloseable {
        private final Thread thread;
        private volatile boolean running = true;
        private volatile double peakRssMb = rssMb();

        MemorySampler() {
            thread = new Thread(() -> {
                while (running) {
                    peakRssMb = Math.max(peakRssMb, rssMb());
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }, "memory-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        double peakRssMb() {
            return Math.max(peakRssMb, rssMb());
        }

        @Override
        public void close() throws InterruptedException {
            running = false;
            thread.interrupt();
            thread.join(1000);
            peakRssMb = Math.max(peakRssMb, rssMb());
        }
    }
}
