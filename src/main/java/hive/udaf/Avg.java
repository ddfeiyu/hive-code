package hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
/**
 * 功能描述: UDAF
 *
 * 什么是UDAF :用户定义聚集函数（user-defined aggregate function）
 *
 * UDAF干什么？ UDAF 接受多个输入数据行，并产生一个输出数据行。像COUNT和MAX这样的函数就是聚集函数。
 *
 *
 * 函数类需要继承UDAF类，计算类Evaluator实现UDAFEvaluator接口
 * Evaluator需要实现UDAFEvaluator的init、iterate、terminatePartial、merge、terminate这几个函数。
     * a）init函数实现接口UDAFEvaluator的init函数。
     * b）iterate接收传入的参数，并进行内部的迭代。其返回类型为boolean。
     * c）terminatePartial无参数，其为iterate函数遍历结束后，返回遍历得到的数据，terminatePartial类似于&nbsp;hadoop的Combiner。
     * d）merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean。
     * e）terminate返回最终的聚集函数结果。
 *
 *
 *
 *
 * @author: dd
 * @date: 2022年06月21日 14:03
 */
public class Avg extends UDAF {

    public static class AvgState {
        private long mCount;
        private double mSum;
    }



    public static class AvgEvaluator implements UDAFEvaluator {
        AvgState state;
        public AvgEvaluator() {
            super();
            state = new AvgState();
            init();
        }
        /**
         * init函数类似于构造函数，用于UDAF的初始化
         */
        public void init() {
            state.mSum = 0;
            state.mCount = 0;
        }
        /**
         * iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
         * @param
         * @return
         */
        public boolean iterate(Double o) {
            if (o != null) {
                state.mSum += o;
                state.mCount++;
            }
            return true;
        }
        /**
         * terminatePartial无参数，其为iterate函数遍历结束后，返回轮转数据， * terminatePartial类似于hadoop的Combiner
         * @return
         */
        public AvgState terminatePartial() {
            // combiner
            return state.mCount == 0 ? null : state;
        }
        /**
         * merge接收terminatePartial的返回结果，进行数据merge操作，
         * @param
         * @return其返回类型为boolean
         */
        public boolean merge(AvgState avgState) {
            if (avgState != null) {
                state.mCount += avgState.mCount;
                state.mSum += avgState.mSum;
            }
            return true;
        }
        /**
         * terminate返回最终的聚集函数结果
         *  @return
         */
        public Double terminate() {
            return state.mCount == 0 ? null : Double.valueOf(state.mSum / state.mCount);
        }
    }
}
