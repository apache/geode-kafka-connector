package kafka;

import java.io.IOException;

public class WorkerAndHerderCluster {

    private JavaProcess workerAndHerder;

    public WorkerAndHerderCluster() {
        workerAndHerder = new JavaProcess(WorkerAndHerderWrapper.class);
    }

    public void start() throws IOException, InterruptedException {
        System.out.println("JASON starting worker");
        workerAndHerder.exec();

    }

    public void stop() {
        workerAndHerder.destroy();
    }
}

