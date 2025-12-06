package org.bytefly;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class SimpleRedisClient {

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 6379);
        OutputStream outputStream = socket.getOutputStream();
        String auth = "*2\r\n$4\r\nAUTH\r\n$8\r\npassword\r\n";
        doCommand(auth,socket,outputStream);

        // set testKey testValue
        String set = "*3\r\n$3\r\nSET\r\n$7\r\ntestKey\r\n$9\r\ntestValue\r\n";
        doCommand(set,socket,outputStream);

        // select 1
        String select= "*2\r\n$6\r\nselect\r\n$1\r\n1\r\n";
        doCommand(select,socket,outputStream);

        String get = "*2\r\n$3\r\nGET\r\n$7\r\ntestKey\r\n";
        doCommand(get,socket,outputStream);
    }

    public static void doCommand(String command, Socket socket, OutputStream outputStream) throws Exception {
        outputStream.write((command).getBytes());
        outputStream.flush();

        InputStream inputStream = socket.getInputStream();
        byte[] bytes = new byte[60];
        inputStream.read(bytes);
        String s1 = new String(bytes);
        System.out.println(s1);
    }



}
