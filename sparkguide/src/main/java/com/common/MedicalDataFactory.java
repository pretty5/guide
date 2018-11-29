package com.common;


import jodd.datetime.JDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * 诊疗信息数据工厂类
 */
public class MedicalDataFactory {

    private Logger logger = LoggerFactory.getLogger("MedicalDataFactory");

    private static List<String> hospitalids;

    private static List<String> diseaseids;

    private static List<String> departmentids;

    private static List<String> doctorids;

    private static int recordnum = 0;

    private static int reimbursenum = 0;

    private static int allreimbursenum = 0;

    private static String recordname;

    private static String reimbursename;

    private static String basepath = "C:\\Users\\Administrator\\Desktop\\智能导诊系统\\智能导诊数据工厂\\";  //造数据所需文件和生成的数据文件存放位置

    /**
     * 创建数据
     * @param allnumber 总数据量
     */
    public void createMedicalData(int allnumber) {
        try {
            hospitalids = FileUtil.readFile(basepath+"医院id.txt");
            diseaseids = FileUtil.readFile(basepath+"疾病id.txt");
            departmentids = FileUtil.readFile(basepath+"科室id.txt");
            doctorids = FileUtil.readFile(basepath+"医生id.txt");
            recordname = UUID.randomUUID().toString()+".txt";
            reimbursename = UUID.randomUUID().toString()+".txt";
        } catch (IOException e) {
            logger.error("[MedicalDataFactory.createMedicalData]",e);
        }
        String nowdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()));
        int cnt = 0;
        while(true) {
            writeGroupDate(100000, nowdate, "1");
            writeGroupDate(1000, nowdate, "2");
            writeGroupDate(1000, nowdate, "2");
            writeGroupDate(1000, nowdate, "2");
            cnt = cnt + 4;
            if(cnt%10000==0)
                logger.info("生成数据量："+cnt+"，"+allreimbursenum);
            if(cnt>allnumber) {
                logger.info("数据生成完成。");
                break;
            }
        }
    }

    /**
     * 生成一组数据并保存磁盘
     * @param maxCost 最大金额
     * @param nowdate 当前日期
     * @param flag 住院门诊标识
     *
     */
    private void writeGroupDate(double maxCost, String nowdate, String flag) {
        String recordid = UUID.randomUUID().toString();
        double allcost = getCost(maxCost,10);

        DecimalFormat df = new DecimalFormat("#.00");

        String[] recordArray = getRecord(flag, recordid, nowdate, df.format(allcost));

        List<String> reimburseList = getReimburse(recordid, recordArray[1], allcost, df);

        try {
            FileUtil.writeFile(basepath+"record/"+recordname, recordArray[0], true);
            FileUtil.writeFile(basepath+"reimburse/"+reimbursename, reimburseList, true);
        } catch (IOException e) {
            logger.error("[MedicalDataFactory.writeGroupDate]",e);
        }
        recordnum++;
        reimbursenum = reimbursenum + reimburseList.size();
        allreimbursenum = allreimbursenum + reimburseList.size();
        if(recordnum==460000) {
            recordname = UUID.randomUUID().toString()+".txt";
            recordnum = 0;
        }
        if(reimbursenum>1200000) {
            reimbursename = UUID.randomUUID().toString()+".txt";
            reimbursenum = 0;
        }
    }

    /**
     * 生成一条就诊信息
     * @param flag 住院门诊标识
     * @param recordid 就诊流水号
     * @param nowdate 当前日期
     * @param cost 总花费
     * @return 1.生成好的就诊信息 2.出院时间
     */
    private String[] getRecord(String flag, String recordid, String nowdate, String cost) {
        StringBuffer sb = new StringBuffer();
        sb.append(recordid + "\t");
        sb.append(getHospitalid() + "\t");
        sb.append(getDiseaseid() + "\t");
        sb.append(getDepartmentid() + "\t");
        sb.append(getDoctorid() + "\t");
        sb.append(flag + "\t");
        String starttime = randomDate("2004-01-01",nowdate);
        sb.append(starttime + "\t");
        String endtime;
        if("1".equals(flag)){
            JDateTime jDateTime = new JDateTime(starttime,"YYYY-MM-DD");
            jDateTime.addDay((int) random(7,45));
            endtime = jDateTime.toString("YYYY-MM-DD");
        }
        else{
            endtime = starttime;
        }
        sb.append(endtime + "\t");
        sb.append(cost + "\t");
        sb.append((int)(Math.random()*2));
        String[] returnValue = new String[]{sb.toString(),endtime};
        return returnValue;
    }

    /**
     * 获取报销记录集合
     * @param recordid 就诊流水号
     * @param reimbursetime 报销时间
     * @param cost 总花费
     * @param df double格式化类
     * @return 报销记录集合
     */
    private List<String> getReimburse(String recordid, String reimbursetime, double cost, DecimalFormat df) {
        cost = cost*0.8;
        List<String> reimburseList = new ArrayList();
        for(int cnt=0; cnt<10; cnt++) {
            double tempcost = getCost(cost,1);
            reimburseList.add(recordid + "\t" + reimbursetime + "\t" + df.format(tempcost));
            cost = cost - tempcost;
            if(cost<1)
                break;
        }
        return reimburseList;
    }

    /**
     * 随机获取一个医院id
     * @return 医院id
     */
    private String getHospitalid() {
        return hospitalids.get((int)(Math.random()*39));
    }

    /**
     * 随机获取一个疾病id
     * @return 疾病id
     */
    private String getDiseaseid() {
        return diseaseids.get((int)(Math.random()*21200));
    }

    /**
     * 随机获取一个科室id
     * @return 科室id
     */
    private String getDepartmentid() {
        return departmentids.get((int)(Math.random()*170));
    }

    /**
     * 随机获取一个医生id
     * @return 医生id
     */
    private String getDoctorid() {
        return doctorids.get((int)(Math.random()*10000));
    }

    /**
     * 获取min~max之间的随机数
     * @param max 最大值
     * @param min 最小值
     * @return 获取到的随机数
     */
    private double getCost(double max, double min) {
        double allcost = Math.random()*max;
        if(allcost<min)
            allcost = min;
        return allcost;
    }

    /**
     * 获取随机日期
     * @param beginDate 开始日期
     * @param endDate 结束日期
     * @return 获取到的日期
     */
    private String randomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            Date start = format.parse(beginDate);// 构造开始日期
            Date end = format.parse(endDate);// 构造结束日期
            if (start.getTime() >= end.getTime())
                return null;
            long date = random(start.getTime(), end.getTime());
            return format.format(new Date(date));
        } catch (Exception e) {
            logger.error("[MedicalDataFactory.randomDate]",e);
            return null;
        }
    }

    private long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }
}
