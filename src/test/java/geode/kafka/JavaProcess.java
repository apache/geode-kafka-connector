package geode.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class JavaProcess {

    public Process process;
    Class classWithMain;

    public JavaProcess(Class classWithmain) {
        this.classWithMain = classWithmain;
    }

    public void exec(String... args) throws IOException, InterruptedException {
        String java = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = classWithMain.getName();

        ProcessBuilder builder = new ProcessBuilder(
                java, "-cp", classpath, className, convertArgsToString(args));

        process = builder.inheritIO().start();
    }

    private String convertArgsToString(String... args) {
        String string = "";
        for(String arg: args) {
            string += arg;
        }
        return string;
    }

    public void waitFor() throws InterruptedException {
        process.waitFor();
    }

    public void destroy() {
        process.destroy();
    }



}