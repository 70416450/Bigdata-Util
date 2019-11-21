package com.bigdata.shell.util;

import com.jcraft.jsch.*;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

/**
 * @author Heaton
 * @email tzytzy70416450@gmail.com
 * @date 2019/11/18 17:07
 * @describe SFTP(Secure File Transfer Protocol)，安全文件传送协议。
 */
@Slf4j
@SuppressWarnings("all")
public class ShellUtil {

    /**
     * SFTP服务器IP地址
     */
    private String host;
    /**
     * SFTP服务器端口
     */
    private int port;
    /**
     * 连接超时时间，单位毫秒
     */
    private int timeout;
    /**
     * 用户名
     */
    private String username;
    /**
     * 密码
     */
    private String password;

    /**
     * Session
     */
    private Session session = null;
    /**
     * Channel
     */
    private ChannelSftp channelSftp = null;
    /**
     * Channel
     */
    private ChannelExec channelExec = null;

    /**
     * SFTP 安全文件传送协议
     *
     * @param host     SFTP服务器IP地址
     * @param port     SFTP服务器端口
     * @param timeout  连接超时时间，单位毫秒
     * @param username 用户名
     * @param password 密码
     */
    public ShellUtil(String host, int port, int timeout, String username, String password) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.username = username;
        this.password = password;
    }

    /**
     * 登陆SFTP服务器
     *
     * @return boolean
     */
    /**
     * @param [mode-->模式名 sftp || exec]
     * @return boolean
     * @describe 登陆SFTP服务器
     */
    public boolean login(String mode) {

        try {
            JSch jsch = new JSch();
            session = jsch.getSession(username, host, port);
            if (password != null) {
                session.setPassword(password);
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.setTimeout(timeout);
            session.connect();
            log.info("sftp session connected");

            log.info("opening channelSftp");
            if ("sftp".equalsIgnoreCase(mode)) {
                channelSftp = (ChannelSftp) session.openChannel(mode);
                channelSftp.connect();
                log.info("Sftpconnected successfully");
            }
            if ("exec".equalsIgnoreCase(mode)) {
                channelExec = (ChannelExec) session.openChannel(mode);
                log.info("Execconnected successfully");
            }
            return true;
        } catch (JSchException e) {
            log.error("shell login failed", e);
            return false;
        }
    }

    /**
     * @param []
     * @return void
     * @describe 登出
     */
    public void logout() {
        if (channelSftp != null) {
            channelSftp.quit();
            channelSftp.disconnect();
        }
        if (channelExec != null) {
            channelExec.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
        log.info("logout successfully");
    }


    /**
     * @param [pathName-->SFTP服务器目录, fileName-->服务器上保存的文件名, localFile-->本地文件]
     * @return boolean
     * @describe 上传文件
     */
    public boolean uploadFile(String pathName, String fileName, String localFile) {

        String currentDir = currentDir();
        if (!changeDir(pathName)) {
            return false;
        }

        try {
            channelSftp.put(localFile, fileName, ChannelSftp.OVERWRITE);
            if (!existFile(fileName)) {
                log.info("upload failed");
                return false;
            }
            log.info("upload successful");
            return true;
        } catch (SftpException e) {
            log.error("upload failed", e);
            return false;
        } finally {
            changeDir(currentDir);
        }
    }

    /**
     * @param [remotePath-->SFTP服务器目录, fileName-->服务器上保存的文件名, localPath-->本地文件夹路径]
     * @return boolean
     * @describe 下载文件
     */
    public boolean downloadFile(String remotePath, String fileName, String localPath) {

        String currentDir = currentDir();
        if (!changeDir(remotePath)) {
            return false;
        }

        try {
            String localFilePath = localPath + File.separator + fileName;
            channelSftp.get(fileName, localFilePath);

            File localFile = new File(localFilePath);
            if (!localFile.exists()) {
                log.info("download file failed");
                return false;
            }
            log.info("download successful");
            return true;
        } catch (SftpException e) {
            log.error("download file failed", e);
            return false;
        } finally {
            changeDir(currentDir);
        }
    }

    /**
     * @param [pathName-->SFTP服务器目录]
     * @return boolean
     * @describe 切换目录
     */
    public boolean changeDir(String pathName) {
        if (pathName == null || pathName.trim().equals("")) {
            log.info("invalid pathName");
            return false;
        }

        try {
            channelSftp.cd(pathName.replaceAll("\\\\", "/"));
            log.info("directory successfully changed,current dir=" + channelSftp.pwd());
            return true;
        } catch (SftpException e) {
            log.error("failed to change directory", e);
            return false;
        }
    }

    /**
     * @param []
     * @return boolean
     * @describe 切换到上一级目录
     */
    public boolean changeToParentDir() {
        return changeDir("..");
    }

    /**
     * 切换到根目录
     *
     * @return boolean
     */
    public boolean changeToHomeDir() {
        String homeDir = null;
        try {
            homeDir = channelSftp.getHome();
        } catch (SftpException e) {
            log.error("can not get home directory", e);
            return false;
        }
        return changeDir(homeDir);
    }

    /**
     * @param [dirName-->SFTP服务器上的目录]
     * @return boolean
     * @describe 创建目录（不能创建联级目录）
     */
    public boolean makeDir(String dirName) {
        try {
            channelSftp.mkdir(dirName);
            log.info("directory successfully created,dir=" + dirName);
            return true;
        } catch (SftpException e) {
            log.error("failed to create directory", e);
            return false;
        }
    }

    /**
     * @param [dirName-->删除文件夹路径]
     * @return boolean
     * @describe X
     */
    public boolean delDir(String dirName) {
        if (!changeDir(dirName)) {
            return false;
        }

        Vector<LsEntry> list = null;
        try {
            list = channelSftp.ls(channelSftp.pwd());
        } catch (SftpException e) {
            log.error("can not list directory", e);
            return false;
        }

        for (LsEntry entry : list) {
            String fileName = entry.getFilename();
            if (!fileName.equals(".") && !fileName.equals("..")) {
                if (entry.getAttrs().isDir()) {
                    delDir(fileName);
                } else {
                    delFile(fileName);
                }
            }
        }

        if (!changeToParentDir()) {
            return false;
        }

        try {
            channelSftp.rmdir(dirName);
            log.info("directory " + dirName + " successfully deleted");
            return true;
        } catch (SftpException e) {
            log.error("failed to delete directory " + dirName, e);
            return false;
        }
    }

    /**
     * @param [fileName-->删除文件路径]
     * @return boolean
     * @describe 删除文件
     */
    public boolean delFile(String fileName) {
        if (fileName == null || fileName.trim().equals("")) {
            log.info("invalid filename");
            return false;
        }
        try {
            channelSftp.rm(fileName);
            log.info("file " + fileName + " successfully deleted");
            return true;
        } catch (SftpException e) {
            log.error("failed to delete file " + fileName, e);
            return false;
        }
    }


    /**
     * @param []
     * @return java.lang.String[]
     * @describe 当前目录下文件及文件夹名称列表
     */
    public String[] ls() {
        return list(Filter.ALL);
    }


    /**
     * @param [pathName-->文件夹]
     * @return java.lang.String[]
     * @describe 指定目录下文件及文件夹名称列表
     */
    public String[] ls(String pathName) {
        String currentDir = currentDir();
        if (!changeDir(pathName)) {
            return new String[0];
        }
        ;
        String[] result = list(Filter.ALL);
        if (!changeDir(currentDir)) {
            return new String[0];
        }
        return result;
    }

    /**
     * @param []
     * @return java.lang.String[]
     * @describe 当前目录下文件名称列表
     */
    public String[] lsFiles() {
        return list(Filter.FILE);
    }

    /**
     * @param [pathName-->文件夹]
     * @return java.lang.String[]
     * @describe 指定目录下文件名称列表
     */
    public String[] lsFiles(String pathName) {
        String currentDir = currentDir();
        if (!changeDir(pathName)) {
            return new String[0];
        }
        ;
        String[] result = list(Filter.FILE);
        if (!changeDir(currentDir)) {
            return new String[0];
        }
        return result;
    }


    /**
     * @param []
     * @return java.lang.String[]
     * @describe 当前目录下文件夹名称列表
     */
    public String[] lsDirs() {
        return list(Filter.DIR);
    }

    /**
     * @param [pathName-->文件夹]
     * @return java.lang.String[]
     * @describe 指指定目录下文件夹名称列表
     */
    public String[] lsDirs(String pathName) {
        String currentDir = currentDir();
        if (!changeDir(pathName)) {
            return new String[0];
        }
        ;
        String[] result = list(Filter.DIR);
        if (!changeDir(currentDir)) {
            return new String[0];
        }
        return result;
    }


    /**
     * @param [name-->名称]
     * @return boolean
     * @describe 当前目录是否存在文件或文件夹
     */
    public boolean exist(String name) {
        return exist(ls(), name);
    }

    /**
     * @param [path-->目录, name-->名称]
     * @return boolean
     * @describe 指定目录下，是否存在文件或文件夹
     */
    public boolean exist(String path, String name) {
        return exist(ls(path), name);
    }

    /**
     * @param [name-->名称]
     * @return boolean
     * @describe 当前目录是否存在文件
     */
    public boolean existFile(String name) {
        return exist(lsFiles(), name);
    }

    /**
     * @param [path-->目录, name-->名称]
     * @return boolean
     * @describe 指定目录下，是否存在文件
     */
    public boolean existFile(String path, String name) {
        return exist(lsFiles(path), name);
    }

    /**
     * @param [name-->文件夹名称]
     * @return boolean
     * @describe 当前目录是否存在文件夹
     */
    public boolean existDir(String name) {
        return exist(lsDirs(), name);
    }


    /**
     * @param [path-->目录, name-->文家夹名称]
     * @return boolean
     * @describe 指定目录下，是否存在文件夹
     */
    public boolean existDir(String path, String name) {
        return exist(lsDirs(path), name);
    }

    /**
     * @param []
     * @return java.lang.String
     * @describe 当前工作目录
     */
    public String currentDir() {
        try {
            return channelSftp.pwd();
        } catch (SftpException e) {
            log.error("failed to get current dir", e);
            return homeDir();
        }
    }


    //------private method ------

    /**
     * 枚举，用于过滤文件和文件夹
     */
    private enum Filter {
        /**
         * 文件及文件夹
         */
        ALL,
        /**
         * 文件
         */
        FILE,
        /**
         * 文件夹
         */
        DIR;
    }


    /**
     * @param [filter-->过滤参数]
     * @return java.lang.String[]
     * @describe 列出当前目录下的文件及文件夹
     */
    private String[] list(Filter filter) {
        Vector<LsEntry> list = null;
        try {
            //ls方法会返回两个特殊的目录，当前目录(.)和父目录(..)
            list = channelSftp.ls(channelSftp.pwd());
        } catch (SftpException e) {
            log.error("can not list directory", e);
            return new String[0];
        }

        List<String> resultList = new ArrayList<String>();
        for (LsEntry entry : list) {
            if (filter(entry, filter)) {
                resultList.add(entry.getFilename());
            }
        }
        return resultList.toArray(new String[0]);
    }

    /**
     * @param [entry-->LsEntry, f-->过滤参数]
     * @return boolean
     * @describe 判断是否是否过滤条件
     */
    private boolean filter(LsEntry entry, Filter f) {
        if (f.equals(Filter.ALL)) {
            return !entry.getFilename().equals(".") && !entry.getFilename().equals("..");
        } else if (f.equals(Filter.FILE)) {
            return !entry.getFilename().equals(".") && !entry.getFilename().equals("..") && !entry.getAttrs().isDir();
        } else if (f.equals(Filter.DIR)) {
            return !entry.getFilename().equals(".") && !entry.getFilename().equals("..") && entry.getAttrs().isDir();
        }
        return false;
    }

    /**
     * @param []
     * @return java.lang.String
     * @describe 根目录
     */
    private String homeDir() {
        try {
            return channelSftp.getHome();
        } catch (SftpException e) {
            return "/";
        }
    }

    /**
     * @param [strArr-->字符串数组, str-->字符串]
     * @return boolean
     * @describe 判断字符串是否存在于数组中
     */
    private boolean exist(String[] strArr, String str) {
        if (strArr == null || strArr.length == 0) {
            return false;
        }
        if (str == null || str.trim().equals("")) {
            return false;
        }
        for (String s : strArr) {
            if (s.equalsIgnoreCase(str)) {
                return true;
            }
        }
        return false;
    }


    /**
     * @param [command-->命令]
     * @return java.lang.String
     * @describe exec模式提交命令
     */
    public int execCmd(String command)
            throws Exception {
        if (session == null) {
            throw new RuntimeException("Session is null!");
        }

        BufferedReader inErr = new BufferedReader(new InputStreamReader(channelExec.getErrStream()));

        byte[] b = new byte[1024];

        channelExec.setCommand(command);
        channelExec.connect();

        String lineErr;
        while((lineErr = inErr.readLine()) != null) {
            log.info(lineErr);
        }

        inErr.close();
        int returnCode = 0;
        if (channelExec.isClosed()) {
            returnCode = channelExec.getExitStatus();
        }
        return returnCode;
    }
}