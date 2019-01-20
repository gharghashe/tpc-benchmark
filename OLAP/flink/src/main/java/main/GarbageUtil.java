package main;


import java.util.Timer;
import java.util.TimerTask;

public class GarbageUtil {

    public static void gc() {
        System.gc();
    }

    public static void taskScheduler() {
        TimerTask a = new TimerTask() {
            @Override
            public void run() {
                gc();
                taskScheduler();
            }
        };
        Timer time = new Timer();
        time.schedule(a, 60000);
    }

}
