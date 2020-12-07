package com.manwei.dataPlatform.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @ClassName: HiveJDBC
 * @Description: TODO
 * @Author chengfei
 * @Date 2020/1/9 14:55
 * @Version 1.0
 **/
public class HiveJDBC {
//    private String driveName = "org.apache.hive.jdbc.HiveDriver";
    private String driveName = "org.apache.hive.jdbc.HiveDriver";
    private static Logger logger = Logger.getLogger(HiveJDBC.class);
    Properties properties = new Properties();
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    public HiveJDBC() throws SQLException, IOException, ClassNotFoundException {
        this.initializeConnection();
    }

    /**
     * @Description:
      * @param
     * @return void
     * @date: 2020/11/26 14:18
     * todo 链接hive
     */
    public void initializeConnection() throws IOException, ClassNotFoundException, SQLException {
        InputStream streamData = HiveJDBC.class.getResourceAsStream("/db.properties");
        properties.load(streamData);
        String hiveDriverConnect = properties.getProperty("hiveDriverConnect");
        logger.info("hive的IP：" + hiveDriverConnect);
        String hiveUserName = properties.getProperty("hiveUserName");
        String hivePassword = properties.getProperty("hivePassword");
        Class.forName(driveName);
        connection = DriverManager.getConnection(hiveDriverConnect, hiveUserName, hivePassword);
    }

    /**
     * @Description: 执行hive SQL语句
      * @param sql
     * @return void
     * @date: 2020/11/26 14:19
     */
    public void executeSQL(String sql) throws SQLException, IOException, ClassNotFoundException {

        logger.info("连接hive成功：" + connection);
        statement = connection.createStatement();
        statement.execute(sql);
        logger.info("sql执行完成");
        closeAll();
    }

    public ResultSet executeQuery2Hive(String sql) throws SQLException {
        statement = connection.createStatement();
        resultSet = statement.executeQuery(sql);
        closeAll();
        return resultSet;
    }

    public void closeAll() throws SQLException {
        statement.close();
        connection.close();
    }
}
