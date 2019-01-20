package main;

import query.*;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Master {

    public static PrintStream resultLog;
    public static PrintStream errorLog;

    static {
        try {
            resultLog = new PrintStream("/flink-result.txt");
            errorLog = new PrintStream("/flink-error.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    static List<BaseQuery> allQuery = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        System.out.println("start GarbageUtil");
        GarbageUtil.taskScheduler();
        Q1 q1 = new Q1();
        allQuery.add(q1);
        Q2 q2 = new Q2();
        allQuery.add(q2);
        Q3 q3 = new Q3();
        allQuery.add(q3);
        Q4 q4 = new Q4();
        allQuery.add(q4);
        Q5 q5 = new Q5();
        allQuery.add(q5);
        Q6 q6 = new Q6();
        allQuery.add(q6);
        Q7 q7 = new Q7();
        allQuery.add(q7);
        Q8 q8 = new Q8();
        allQuery.add(q8);
        Q9 q9 = new Q9();
        allQuery.add(q9);
        Q10 q10 = new Q10();
        allQuery.add(q10);
        Q11 q11 = new Q11();
        allQuery.add(q11);
        Q12 q12 = new Q12();
        allQuery.add(q12);
        Q13 q13 = new Q13();
        allQuery.add(q13);
        Q14 q14 = new Q14();
        allQuery.add(q14);
        Q15 q15 = new Q15();
        allQuery.add(q15);
        Q16 q16 = new Q16();
        allQuery.add(q16);
        Q17 q17 = new Q17();
        allQuery.add(q17);
        Q18 q18 = new Q18();
        allQuery.add(q18);
        Q19 q19 = new Q19();
        allQuery.add(q19);
        Q20 q20 = new Q20();
        allQuery.add(q20);
        Q21 q21 = new Q21();
        allQuery.add(q21);
        Q22 q22 = new Q22();
        allQuery.add(q22);
        System.out.println("queries added to list");
        System.out.println("insert query number to execute(split with space) : ");

        Scanner input = new Scanner(System.in);
        String q = input.nextLine();
        List<Integer> numbers = new ArrayList<>();
        for (String s : q.split(" ")) {
            numbers.add(Integer.parseInt(s));
        }

        for (Integer number : numbers) {
            System.out.println("start to execute query with number " + number + "...");
            allQuery.get(number).startQueryExec();
            System.out.println("task for query with number " + number + " stared");
        }
        System.out.println("all query running, waiting for collect result with flink");
    }

}
