package com.wangheng.utils.file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class MyFile {
    public static void main(String[] args) throws IOException {
        File f1 = new File("/home/wangheng/Documents/dept.txt");
        File f2 = new File("/home/wangheng/Documents/emp.txt");

        FileWriter w1 = new FileWriter(f1);
        FileWriter w2 = new FileWriter(f2);

        Random random = new Random();
        int a = random.nextInt();

        for(int i = 0;i<10;i++){
            w2.write((random.nextInt(40) + 7000) +"\t"+ getRandomFirstName() +"\t"+ getRandomFirstName() +"\t"
                    + (random.nextInt(20) + 7000) +"\t"+ "1988-2-1" + "\t" + 800.00 +"\t"+ i*10 + "\n");
            w1.write(i*10 +"\t" + getRandomFirstName() +"\t" + getRandomFirstName() + "\n");
        }
        w1.close();
        w2.close();
    }

    private static String getRandomFirstName(){
        Random r = new Random();
        //产生1个名字
        int name_len = r.nextInt(6) + 4;
        StringBuilder firstName = new StringBuilder();
        for(int j = 0;j<name_len;j++){
            int as = r.nextInt(26) + 65;
            char one_char = (char) as;
            firstName.append(one_char);
        }
        return firstName.toString();
    }
}
