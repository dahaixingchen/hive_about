package com.manwei.dataPlatform;

import com.manwei.dataPlatform.util.HiveJDBC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * @ClassName: Data2Hive
 * @Author chengfei
 * @Date 2020/12/7 19:56
 * @Description: TODO
 **/
public class Data2Hive {
    static Logger logger = LoggerFactory.getLogger(Data2Hive.class);
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        //链接hive
        HiveJDBC hiveJDBC = new HiveJDBC();
        //        String sqlStr = "load data local inpath '/root/data/log/" + LocalDate.now().plusDays(-1).toString() +".log' overwrite into table ods_cxy.action_data " +
//                "partition(department = 'cxy',datetime = '"+ LocalDate.now().plusDays(-1).toString() + "')";

        //导入hdfs上的文件
        String sqlStr = "load data inpath '/2020-12-07.log' overwrite into table ods_cxy.action_data " +
                "partition(department = 'cxy',datetime = '"+ LocalDate.now().plusDays(-1).toString() + "')";
        hiveJDBC.executeSQL(sqlStr);
        logger.info("数据导入成功");
    }
}
