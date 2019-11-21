package com.bigdata.sqoop.util;


import com.bigdata.shell.util.ShellUtil;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class SqoopUtil {

    @Resource
    ShellUtil shellUtil;

    public int test() {
        shellUtil.login("exec");
        String cmd = "sqoop import " +
                "--connect jdbc:mysql://10.28.17.238:3306/bigdata_new " +
                "--username root " +
                "--password smcdyanfa " +
                "--table pb_test " +
                "--target-dir /myTest/bigdata_new " +
                "--delete-target-dir " +
                "--num-mappers 1 " +
                "--fields-terminated-by '\t' ";
        int s = 0;
        try {
            s = shellUtil.execCmd(cmd);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(cmd);


        shellUtil.logout();
        return s;
    }
}
