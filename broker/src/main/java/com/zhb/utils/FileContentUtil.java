package com.zhb.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @Author idea
 * @Date: Created in 22:48 2024/3/26
 * @Description 简化版本的文件读写工具
 */
public class FileContentUtil {

    public static String readFromFile(String path) {
        try(BufferedReader in = new BufferedReader(new FileReader(path))) {
            StringBuffer stb = new StringBuffer();
            while (in.ready()) {
                stb.append(in.readLine());
            }
            return stb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void overWriteToFile(String path, String content) {
        try (FileWriter fileWriter = new FileWriter(path)) {
            fileWriter.write(content);
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
