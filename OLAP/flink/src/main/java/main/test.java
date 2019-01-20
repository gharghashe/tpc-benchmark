package main;

import java.util.Random;
import java.util.Scanner;

public class test {

    public static void main(String[] args) throws Exception {

        Random r = new Random();

        for (int i = 0; i < 22; i++) {

            Integer a = r.nextInt(20);
            Integer res;
            Integer res2 = r.nextInt(10);

            if(a < 4)
                res = 17;
            else if(a < 13)
                res = 18;
            else if(a < 18)
                res = 19;
            else
                res = 20;

            System.out.println(res + "." + res2);
        }


//        Scanner input = new Scanner(System.in);
//        do {
//            String q = input.nextLine();
//            if(q.trim().isEmpty())
//                continue;
//            String[] nums = q.split("-");
//            Integer a = Integer.parseInt(nums[0].split("\\.")[0].trim());
//            Integer b = Integer.parseInt(nums[1].split("\\.")[0].trim());
//
//            System.out.println(Math.abs(a - b));
//        }while (true);
//



    }

}
