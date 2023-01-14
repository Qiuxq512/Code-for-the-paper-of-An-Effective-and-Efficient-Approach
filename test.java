package com.fpg;

import com.util.DBUtil;
import com.util.getData;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class test {
    private static String user = "root";
    private static String password = "root";
    private static String dbName = "73426";
    //private static String table = "sheet3";// 标签数据xjfddy2
    private static String tableSrc = "sheet48" +
            "";// 原数据
//    private static int rand = 1;
    private static HashSet<String> mapResult = new HashSet<>();

    public static HashSet<String> VALIDSET = new HashSet<>();

    public static void main(String[] args) {
        // 总数据：73426
        List<String> wholeData = null;
        int sampleNum = 1500;    // 样本数量
        int times = 1;          // 抽样次数
        String preName = "C:\\Users\\qiuxq\\Desktop\\A\\";  // 输出路径

        addSet();

        try
        {

            DBUtil db = new DBUtil(dbName, user, password);// connect to DB
            getData getData = new getData();
            wholeData = getData.getDataFromDB2(db, false, tableSrc);//无前缀
            System.out.println("总样本数量：" + wholeData.size());

            System.out.println("样本数量：" + sampleNum);
            System.out.println("抽样次数：" + times);
            sample(wholeData,sampleNum,times,preName+sampleNum+"_",false);

        }
        catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void addSet() {
        VALIDSET.add("TMPD_G2;--->DXPD_G1;");
        VALIDSET.add("TMPD_G3;--->DXPD_G1;");
        VALIDSET.add("TMPD_G4;--->DXPD_G1;");
        VALIDSET.add("TMPD_G5;--->DXPD_G1;");
        VALIDSET.add("TMPD_G3;--->DXPD_G2;");
        VALIDSET.add("TMPD_G4;--->DXPD_G2;");
        VALIDSET.add("TMPD_G5;--->DXPD_G2;");
        VALIDSET.add("TMPD_G4;--->DXPD_G3;");
        VALIDSET.add("TMPD_G5;--->DXPD_G3;");
        VALIDSET.add("TMPD_G5;--->DXPD_G4;");
        VALIDSET.add("PMGX_RSS;--->BTZD_LT;");
        VALIDSET.add("PMGX_ST;--->BTZD_LT;");
        VALIDSET.add("PMGX_LT;--->BTZD_NT;");
        VALIDSET.add("PMGX_RNN;--->BTZD_NT;");
        VALIDSET.add("PMGX_RNR--->BTZD_NT;");
        VALIDSET.add("PMGX_RSR;--->BTZD_NT;");
        VALIDSET.add("PMGX_RSS;--->BTZD_NT;");
        VALIDSET.add("PMGX_RT;--->BTZD_NT;");
        VALIDSET.add("PMGX_SNN;--->BTZD_NT;");
        VALIDSET.add("PMGX_SNS;--->BTZD_NT;");
        VALIDSET.add("PMGX_SRS;--->BTZD_NT;");
        VALIDSET.add("PMGX_ST;--->BTZD_NT;");
        VALIDSET.add("PMGX_LT;--->BTZD_QR;");
        VALIDSET.add("PMGX_NSN;--->BTZD_QR;");
        VALIDSET.add("PMGX_NT;--->BTZD_QR;");
        VALIDSET.add("PMGX_SNN;--->BTZD_QR;");
        VALIDSET.add("PMGX_SNS;--->BTZD_QR;");
        VALIDSET.add("PMGX_SRS;--->BTZD_QR;");
        VALIDSET.add("PMGX_ST;--->BTZD_QR;");
        VALIDSET.add("PMGX_NT;--->BTZD_RT;");
        VALIDSET.add("PMGX_RNN;--->BTZD_SNT;");
        VALIDSET.add("PMGX_RT;--->BTZD_SNT;");
        VALIDSET.add("PMGX_LT;--->BTZD_SR;");
        VALIDSET.add("PMGX_NT;--->BTZD_SR;");
        VALIDSET.add("PMGX_ST;--->BTZD_SR;");
        VALIDSET.add("PMGX_XS;--->BTZD_SR;");
        VALIDSET.add("PMGX_RSS;--->BTZD_SS;");
        VALIDSET.add("PMGX_LT;--->BTZD_ST;");
        VALIDSET.add("PMGX_NSN;--->BTZD_ST;");
        VALIDSET.add("PMGX_NT;--->BTZD_ST;");
        VALIDSET.add("PMGX_RNN;--->BTZD_ST;");
        VALIDSET.add("PMGX_RNR;--->BTZD_ST;");
        VALIDSET.add("PMGX_RT;--->BTZD_ST;");
        VALIDSET.add("PMGX_RT;--->BTZD_SXS;");
        VALIDSET.add("PMGX_LT;--->BTZD_SXR;");
        VALIDSET.add("PMGX_NSN;--->BTZD_SXR;");
        VALIDSET.add("PMGX_NSS;--->BTZD_SXR;");
        VALIDSET.add("PMGX_NT;--->BTZD_SXR;");
        VALIDSET.add("PMGX_SNN;--->BTZD_SXR;");
        VALIDSET.add("PMGX_SNS;--->BTZD_SXR;");
        VALIDSET.add("PMGX_ST;--->BTZD_SXR;");
        VALIDSET.add("PMGX_RNN;--->BTZD_SXT;");
        VALIDSET.add("PMGX_RSS;--->BTZD_SXT;");
        VALIDSET.add("PMGX_LT;--->BTZD_ZR;");
        VALIDSET.add("PMGX_NSN;--->BTZD_ZR;");
        VALIDSET.add("PMGX_NSS;--->BTZD_ZR;");
        VALIDSET.add("PMGX_NT;--->BTZD_ZR;");
        VALIDSET.add("PMGX_SNN;--->BTZD_ZR;");
        VALIDSET.add("PMGX_SNS;--->BTZD_ZR;");
        VALIDSET.add("PMGX_SRS;--->BTZD_ZR;");
        VALIDSET.add("PMGX_ST;--->BTZD_ZR;");
        VALIDSET.add("TMPD_G5;--->GGBZL_1;");
        VALIDSET.add("GGBZL_1;--->TMPD_G5;");
        VALIDSET.add("BTZD_ZR;--->PMGX_ST;");
        VALIDSET.add("BTZD_ZR;--->PMGX_SRS;");
        VALIDSET.add("BTZD_ZR;--->PMGX_SNS;");
        VALIDSET.add("BTZD_ZR;--->PMGX_SNN;");
        VALIDSET.add("BTZD_ZR;--->PMGX_NT;");
        VALIDSET.add("BTZD_ZR;--->PMGX_NSS;");
        VALIDSET.add("BTZD_ZR;--->PMGX_NSN;");
        VALIDSET.add("BTZD_ZR;--->PMGX_LT;");
        VALIDSET.add("BTZD_XST;--->PMGX_RSS;");
        VALIDSET.add("BTZD_XST;--->PMGX_RNN;");
        VALIDSET.add("BTZD_XSR;--->PMGX_ST;");
        VALIDSET.add("BTZD_XSR;--->PMGX_SNS;");
        VALIDSET.add("BTZD_XSR;--->PMGX_SNN;");
        VALIDSET.add("BTZD_XSR;--->PMGX_NT;");
        VALIDSET.add("BTZD_XSR;--->PMGX_NSS;");
        VALIDSET.add("BTZD_XSR;--->PMGX_NSN;");
        VALIDSET.add("BTZD_XSR;--->PMGX_LT;");
        VALIDSET.add("BTZD_SXS;--->PMGX_RT;");
        VALIDSET.add("BTZD_ST;--->PMGX_RT;");
        VALIDSET.add("BTZD_ST;--->PMGX_RNR;");
        VALIDSET.add("BTZD_ST;--->PMGX_RNN;");
        VALIDSET.add("BTZD_ST;--->PMGX_NT;");
        VALIDSET.add("BTZD_ST;--->PMGX_NSN;");
        VALIDSET.add("BTZD_ST;--->PMGX_LT;");
        VALIDSET.add("BTZD_SS;--->PMGX_RSS;");
        VALIDSET.add("BTZD_SR;--->PMGX_XS;");
        VALIDSET.add("BTZD_SR;--->PMGX_ST;");
        VALIDSET.add("BTZD_SR;--->PMGX_NT;");
        VALIDSET.add("BTZD_SR;--->PMGX_LT;");
        VALIDSET.add("BTZD_SNT;--->PMGX_RT;");
        VALIDSET.add("BTZD_SNT;--->PMGX_RNN;");
        VALIDSET.add("BTZD_RT;--->PMGX_NT;");
        VALIDSET.add("BTZD_QR;--->PMGX_ST;");
        VALIDSET.add("BTZD_QR;--->PMGX_SRS;");
        VALIDSET.add("BTZD_QR;--->PMGX_SNS;");
        VALIDSET.add("BTZD_QR;--->PMGX_SNN;");
        VALIDSET.add("BTZD_QR;--->PMGX_NT;");
        VALIDSET.add("BTZD_QR;--->PMGX_NSN;");
        VALIDSET.add("BTZD_QR;--->PMGX_LT;");
        VALIDSET.add("BTZD_NT;--->PMGX_ST;");
        VALIDSET.add("BTZD_NT;--->PMGX_SRS;");
        VALIDSET.add("BTZD_NT;--->PMGX_SNS;");
        VALIDSET.add("BTZD_NT;--->PMGX_SNN;");
        VALIDSET.add("BTZD_NT;--->PMGX_RT;");
        VALIDSET.add("BTZD_NT;--->PMGX_RSS;");
        VALIDSET.add("BTZD_NT;--->PMGX_RSR;");
        VALIDSET.add("BTZD_NT;--->PMGX_RNR");
        VALIDSET.add("BTZD_NT;--->PMGX_RNN;");
        VALIDSET.add("BTZD_NT;--->PMGX_LT;");
        VALIDSET.add("BTZD_LT;--->PMGX_ST;");
        VALIDSET.add("BTZD_LT;--->PMGX_RSS;");
        VALIDSET.add("DXPD_G4;--->TMPD_G5;");
        VALIDSET.add("DXPD_G3;--->TMPD_G5;");
        VALIDSET.add("DXPD_G3;--->TMPD_G4;");
        VALIDSET.add("DXPD_G2;--->TMPD_G5;");
        VALIDSET.add("DXPD_G2;--->TMPD_G4;");
        VALIDSET.add("DXPD_G2;--->TMPD_G3;");
        VALIDSET.add("DXPD_G1;--->TMPD_G5;");
        VALIDSET.add("DXPD_G1;--->TMPD_G4;");
        VALIDSET.add("DXPD_G1;--->TMPD_G3;");
        VALIDSET.add("DXPD_G1;--->TMPD_G2;");
    }

    /**
     * 循环抽样，并输出映射结果
     * @param wholeData 总数据
     * @param sampleNum 一次抽样的样本数量
     * @param times 抽样次数
     * @param preName 输出路径和前缀名，如 "C:\\out\\to\\path\\sample100"
     * @param out 是否把样本输出为Text
     */
    public static void sample(List<String> wholeData,int sampleNum, int times, String preName, boolean out){
        // 存放数据集下标的集合
        HashSet<Integer> index = new HashSet<>();
        int size = wholeData.size();
        for (int i = 0; i < size; i++) {
            index.add(i);
        }
        BufferedWriter bw;
        // 进行times次抽样和映射分析
        for (int j = 0; j < times; j++) {
            System.out.printf("执行第%d次抽样...\n",j+1);
            // 开始一次抽样
            List<String> sampleData = new ArrayList<>();
            Random random = new Random();

            while (sampleData.size() < sampleNum){
                random.setSeed(System.currentTimeMillis());
                int rand = random.nextInt(size + 1);
//                int rand = (int) (Math.random() * size);
                if(index.contains(rand))
                    sampleData.add(wholeData.get(rand));
                index.remove(rand);

            }
            //完成一次抽样，开始分析结果
            System.out.printf("完成数量为%d的抽样，", sampleData.size());
            // 是否把样本输出为txt
            if(out){
                String sampleText = preName+"RawSample"+(j+1)+".txt";
                List2Txt(sampleText,sampleData);
                System.out.println("样本已保存："+sampleText);
            }
            System.out.println("开始分析结果...");
            Mapping(sampleData,preName+(j+1),false);
        }

    }

    /**
     * 处理映射关系
     * @param data 数据表
     * @param preName 输出文件名
     * @param Uniconfi 不重复的txt中是否需要置信度
     */
    public static void Mapping(List<String> data,String preName, boolean Uniconfi){
        FPG fpg = new FPG();
        FileWriter Fwriter, UFwriter;
        FileWriter Rwriter, Vwriter;
        try {

            Map<String, Integer> map = fpg.fpgMap(data);

            String fpgResult = preName + "_fpgResult_" + fpg.support + "_" + fpg.confidence + "_" + ".txt";
            String fpgResult_unique = preName + "_Unique-fpgResult_" + fpg.support + "_" + fpg.confidence + "_" + ".txt";
            String detailResult = preName + "_Detail_" + fpg.support + "_" + fpg.confidence + "_" + ".txt";
            String validResult = preName + "_valid_" + fpg.support + "_" + fpg.confidence + "_" + ".txt";

            Fwriter = new FileWriter(fpgResult);
            UFwriter = new FileWriter(fpgResult_unique);
            Rwriter = new FileWriter(detailResult);
            Vwriter = new FileWriter(validResult);

            Map<String, Double> results = fpg.getRelation(map, data);
            System.out.println("**********************************************");
            System.out.println("分析完成，开始输出txt...");
            for (Map.Entry<String, Double> entry : results.entrySet()) {
                String key = entry.getKey();
                double value = entry.getValue();

                Rwriter.write(key + ":" + value);
                Rwriter.write("\r\n");

                Fwriter.write(key + ":" + value);
                Fwriter.write("\r\n");
                Fwriter.flush();

                if(! mapResult.contains(key))
                {
                    mapResult.add(key);
                    if(Uniconfi)
                        UFwriter.write(key + ":" + value);
                    else
                        UFwriter.write(key);
                    UFwriter.write("\r\n");
                    UFwriter.flush();
                }

                if (VALIDSET.contains(key)){
                    Vwriter.write(key + ":" + value);
                    Vwriter.write("\r\n");
                    Vwriter.flush();
                }
                String firstStr = key.split(";--->")[0];
                String secondStr = key.split(";--->")[1];
                for (int k = 0; k < data.size(); k++) {
                    if (data.get(k).contains(firstStr) && data.get(k).contains(secondStr)) {
//                        System.out.println(data.get(k));
                        Rwriter.write(data.get(k));
                        Rwriter.write("\r\n");
                        Rwriter.flush();
                    }
                }
            }
            Fwriter.close();
            UFwriter.close();
            Rwriter.close();
            Vwriter.close();
            System.out.println("完成所有任务。");
        }catch (Exception e){
            System.out.println("ERROR: 处理映射关系时出错！");
        }
    }

    public static void List2Txt(String path, List<String> list){
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(path));
            for (String s:list
                 ) {
                bw.write(s);
                bw.newLine();
                bw.flush();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
