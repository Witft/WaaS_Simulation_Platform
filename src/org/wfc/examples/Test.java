package org.wfc.examples;

public class Test {
    public static void main(String[] args) {
        String[] argIn = new String[1];
        argIn[0] = new String();
        for(int i = 0; i < 3; i++){
            MyWFCExample3.main(argIn);
        }
        System.out.println(argIn[0]);
    }
}
