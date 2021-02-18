package com.example.josim

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import kotlin.math.abs
import kotlin.math.round

class Josim (val corp_name: String){
//    var listType: TypeToken<MutableList<corp_data>> = object : TypeToken<MutableList<corp_data>>(){}
//    fun init(){
//        var gson = GsonBuilder().create()
//    }

    //Class variables are not assigned
    private lateinit var corp_cls : String
    private lateinit var data_status : Array<Int>
    private lateinit var sales_amount : Array<Long?>
    private lateinit var net_income_amount : Array<Long?>
    private lateinit var ownership_amount : Array<Long?>
    private lateinit var business_profit_amount : Array<Long?>
    private lateinit var capital_amount : Array<Long?>
    private lateinit var total_assets_amount : Array<Long?>
//
//    private fun getJson() : Unit{
//        gson.fromJson(,corp_rprts)
//    }

    private fun sumNullable(vararg longs: Long?) : Long{
        var sum = 0L
        for (long in longs) {
            sum += (long?:throw IllegalArgumentException("values for sum contain null"))
        }
        return sum
    }

    private val cur_year_rprt_cnt = data_status.slice(0 until 4).sum()  // 올해
    private val last_year_rprt_cnt = data_status.slice(4 until 8).sum()  // 작년

    // 조건값
    //항목 1 조건 - 매출액 - 코스피: 50 억 / 코스닥: 30억
    private val sales_limit = if (corp_cls == "Y") 5000000000 else 3000000000
    //항목 2 조건 - 법인세 비용차감전계속 사업손실: -10억
    private val net_income_limit = -1000000000
    //항목 4 조건 - 자본잠식: 10억, 잠식률: 50%
    private val capital_erosion_limit = 1000000000
    private val capital_erosion_rate_limit = 50

    //  올해부터 몇 년 전인가 0          - 1         - 1             - 2             - 2           - 3           - 3            - 4          - 4               - 5
    //  인덱스 번호          0 2 4 6 -> 1 3 5 7 -> 8 10 12 14 -> 9 11 13 15 -> 16 18 20 22 -> 17 19 21 23 -> 24 26 28 30 -> 25 27 29 31 -> 32 34 36 38 -> 33 35 37 39

    //함수 1 : 매출액
    fun test_sales_amount(): Unit {
        println("1. 매출액 기준 조건 검사")

        var sales_status : String? = when(cur_year_rprt_cnt){
            0-> {
                when (last_year_rprt_cnt) {
                    3 -> if ((sales_amount[22]?:throw IllegalArgumentException("-2 year sales amount is null")) < sales_limit) "상폐 조건 : " else "관리 조건 : "
                    4 -> if ((sales_amount[14]?:throw IllegalArgumentException("-1 year sales amount is null")) < sales_limit) "상폐 조건 : " else "관리 조건 : "
                    else -> throw IllegalArgumentException("Prev year reports are less than 3.") //상장된지 2년 이하인 기업은 미리 걸러내서 점수를 내지 않음을 표기해야 함
                }
            }
            else -> if((sales_amount[14]?:throw IllegalArgumentException("sales amount for prev year is null")) < sales_limit) "상폐 조건 : " else "관리 조건 : "
        }

        val index = arrayOf(0, 2, 4)
        val temp_array : Array<Long?> = arrayOfNulls(3)
        var temp_array_size : Int = 0
        for (i in index){
            sales_amount[i]?.run { // if(sales_amount[i] != null 과 같음
                temp_array[temp_array_size++] = sales_amount[i]
            }
        }

        var sales_score : Double =
                if (temp_array_size != 0) {
            when {
                temp_array.slice(0 until temp_array_size).filterNotNull().sum() >= temp_array_size.toDouble() * sales_limit / 4 -> {
                    when (temp_array_size) {
                        1 -> (1 - (sales_amount[0]?:throw IllegalArgumentException("null in sales_amount[0]")).times(4).toDouble() / sales_limit) * 100
                        2 -> (1 - sumNullable(sales_amount[0], sales_amount[2]?.times(3)).toDouble() / sales_limit) * 100
                        3 -> (1 - sumNullable(sales_amount[0], sales_amount[2], sales_amount[4]).toDouble() / sales_limit) * 100
                        else -> { //나올 일 없음.
                            throw IllegalArgumentException("sales_score: null\ntemp_array_size: $temp_array_size (not in [1,2,3])")
                        }
                    }
                }
                temp_array_size < 3 -> (1 - (sales_amount[0]?:throw IllegalArgumentException("null in sales_amount[0])")).times(4).toDouble() / (temp_array_size.toDouble() * sales_limit / 4)) * 100
                else -> 0.0
            }
        }
        else 0.0

        sales_score = if (sales_score < 0) 0.0 else sales_score
        println("${sales_status} ${round(sales_score*100)/100} %")
    }

    //함수 2 : 법인세 비용차감전계속 사업손실
    fun test_net_income() : Unit {
        println("2. 법인세 비용차감전계속 사업손실 검사")
        val bz_report_indices = arrayOf(38, 30, 22, 14) // 4,3,2,1년 전 사업보고서 인덱스
        val score_array : Array<Int> = Array(bz_report_indices.size) {0}
        if (corp_cls == "K") {
            for ((score_index, bz_rp_index) in bz_report_indices.withIndex()) {
                val condition = net_income_amount[bz_rp_index]?.let {
                    (it < net_income_limit) and (abs(it) > ((ownership_amount[bz_rp_index]?.toDouble()
                            ?: throw IllegalArgumentException("ownership_amount[${bz_rp_index}] is null")) / 2))
                }
                        ?: throw IllegalArgumentException("net_income_amount[${bz_rp_index}] is null")
                if (condition) score_array[score_index] = 1 // 2번 조건 만족시 1
            }
            when (cur_year_rprt_cnt) {
                0 -> {
                    when (last_year_rprt_cnt) {
                        3 -> {
                            val last_year_net_income = sumNullable(net_income_amount[8], net_income_amount[10], net_income_amount[12])
                            val condition = (last_year_net_income < (net_income_limit.toDouble() * 3 / 4)) and (abs(last_year_net_income) > ((ownership_amount[12]?.toDouble()
                                    ?: throw IllegalArgumentException("ownership_amount[12] is null")) / 2))
                            when {
                                (score_array.sum() == 2) and (score_array[2] == 1) -> {
                                    if (condition) {
                                        println("관리 조건 : 100%")
                                        println("상폐 조건 : 87.5%")
                                    } else {
                                        println("관리 조건 : 100%")
                                        println("상폐 조건 : 0%")
                                    }
                                }
                                score_array.sum() == 0 -> {
                                    if (condition) {
                                        println("관리 조건 : 37.5%")
                                        println("상폐 조건 : 0%")
                                    } else {
                                        println("관리 조건 : 0%")
                                        println("상폐 조건 : 0%")
                                    }
                                }
                                score_array[1] + score_array[2] == 1 -> {
                                    if (condition) {
                                        println("관리 조건 : 87.5%")
                                        println("상폐 조건 : 0%")
                                    } else {
                                        println("관리 조건 : 0%")
                                        println("상폐 조건 : 0%")
                                    }
                                }
                                else -> {
                                    if (condition) {
                                        println("관리 조건 : 37.5%")
                                        println("상폐 조건 : 0%")
                                    } else {
                                        println("관리 조건 : 0%")
                                        println("상폐 조건 : 0%")
                                    }
                                }
                            }
                        }
                        4 -> {
                            when {
                                (score_array.sum() >= 3) and (score_array[2] == 1) -> {
                                    println("관리 조건 : 100%")
                                    println("상폐 조건 : 100%")
                                }
                                (score_array.slice(1..score_array.lastIndex).sum() >= 2) and (score_array[3] == 1) -> {
                                    println("관리 조건 : 100%")
                                    println("상폐 조건 : 0%")
                                }
                                (score_array.slice(2..score_array.lastIndex).sum() >= 1) and (score_array[3] == 1) -> {
                                    println("관리 조건 : 50%")
                                    println("상폐 조건 : 0%")
                                }
                                else -> {
                                    println("관리 조건 : 0%")
                                    println("상폐 조건 : 0%")
                                }
                            }
                        }
                    }
                }
                1 -> {
                    val condition = net_income_amount[0]?.let {
                        (it < (net_income_limit.toDouble() * 1 / 4)) and (abs(it) > (ownership_amount[0]?.toDouble()
                                ?: throw IllegalArgumentException("ownership_amount[0] is null")) / 2)
                    }
                            ?: throw IllegalArgumentException("net_income_amount[0] is null")
                    when {
                        (score_array.slice(1..score_array.lastIndex).sum() == 2) and (score_array[3] == 1) -> {
                            if (condition) {
                                println("관리 조건 : 100%")
                                println("상폐 조건 : 25%")
                            } else {
                                println("관리 조건 : 100%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        score_array.sum() == 0 -> {
                            if (condition) {
                                println("관리 조건 : 12.5%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 0%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        score_array.slice(2..score_array.lastIndex).sum() == 1 -> {
                            if (condition) {
                                println("관리 조건 : 62.5%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 50%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        else -> {
                            if (condition) {
                                println("관리 조건 : 12.5%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 0%")
                                println("상폐 조건 : 0%")
                            }
                        }
                    }
                }
                2 -> {
                    val condition = sumNullable(net_income_amount[0], net_income_amount[2]).let {
                        (it < (net_income_limit.toDouble() * 2 / 4)) and (abs(it) > (ownership_amount[2]?.toDouble()
                                ?: throw IllegalArgumentException("ownership_amount[2] is null")) / 2)
                    }

                    when {
                        (score_array.slice(1..score_array.lastIndex).sum() == 2) and (score_array[3] == 1) -> {
                            if (condition) {
                                println("관리 조건 : 100%")
                                println("상폐 조건 : 50%")
                            } else {
                                println("관리 조건 : 100%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        score_array.sum() == 0 -> {
                            if (condition) {
                                println("관리 조건 : 25%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 0%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        score_array.slice(2..score_array.lastIndex).sum() == 1 -> {
                            if (condition) {
                                println("관리 조건 : 75%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 50%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        else -> {
                            if (condition) {
                                println("관리 조건 : 25%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 0%")
                                println("상폐 조건 : 0%")
                            }
                        }
                    }
                }
                3 -> {
                    val condition = sumNullable(net_income_amount[0], net_income_amount[2], net_income_amount[4]).let {
                        (it < (net_income_limit.toDouble() * 3 / 4)) and (abs(it) > (ownership_amount[4]?.toDouble()
                                ?: throw IllegalArgumentException("ownership_amount[4] is null")) / 2)
                    }
                    when {
                        (score_array.slice(1..score_array.lastIndex).sum() == 2) and (score_array[3] == 1) -> {
                            if (condition) {
                                println("관리 조건 : 100%")
                                println("상폐 조건 : 75%")
                            } else {
                                println("관리 조건 : 100%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        score_array.sum() == 0 -> {
                            if (condition) {
                                println("관리 조건 : 37.5%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 0%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        score_array.slice(2..score_array.lastIndex).sum() == 1 -> {
                            if (condition) {
                                println("관리 조건 : 87.5%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 50%")
                                println("상폐 조건 : 0%")
                            }
                        }
                        else -> {
                            if (condition) {
                                println("관리 조건 : 37.5%")
                                println("상폐 조건 : 0%")
                            } else {
                                println("관리 조건 : 0%")
                                println("상폐 조건 : 0%")
                            }
                        }
                    }
                }
            }
        }
        else println("코스닥만 해당")
    }

    //함수 3 : 장기간 영업손실
    fun test_longterm_profit(): Unit {
        println("3. 장기간 영업손실 검사")
        if(corp_cls == "K"){
            if(cur_year_rprt_cnt == 0) {
                if (last_year_rprt_cnt > 3) { // 징검다리로 영업손실이 나는 건 상관 없는 건가요?
                    if ((business_profit_amount[14]
                                    ?: throw IllegalArgumentException("-1 year business_profit_amount is null")) < 0) {
                        if ((business_profit_amount[22]
                                        ?: throw IllegalArgumentException("-2 year business_profit_amount is null")) < 0) {
                            if ((business_profit_amount[30]
                                            ?: throw IllegalArgumentException("-3 year business_profit_amount is null")) < 0) {
                                if ((business_profit_amount[38]
                                                ?: throw IllegalArgumentException("-4 year business_profit_amount is null")) < 0) println("상폐 조건 충족 시작")
                                else println("관리 조건 75% 충족")
                            } else println("관리 조건 50% 충족")
                        } else println("관리 조건 25% 충족")
                    } else println("해당 사항 없음")
                }
                else {
                    val penalty = sumNullable(business_profit_amount[8], business_profit_amount[10], business_profit_amount[12]) 
                    var percentile = if(penalty < 0) 18 else 0
                    if((business_profit_amount[22]?: throw IllegalArgumentException("-2 year business_profit_amount is null")) < 0){
                        if((business_profit_amount[30]?: throw IllegalArgumentException("-3 year business_profit_amount is null")) < 0){
                            if((business_profit_amount[38]?: throw IllegalArgumentException("-4 year business_profit_amount is null")) < 0) {
                                if ((business_profit_amount[39]
                                                ?: throw IllegalArgumentException("-5 year business_profit_amount is null")) < 0) {
                                    if (penalty < 0) println("상폐 조건 75% 충족")
                                }
                                else {
                                    percentile += 75
                                    println("관리 조건 $percentile% 충족")
                                }
                            }
                            else {
                                percentile += 50
                                println("관리 조건 $percentile% 충족")
                            }
                        }
                        else {
                            percentile += 25
                            println("관리 조건 $percentile% 충족")
                        }
                    } else println("해당 사항 없음")
                }
            }
            else{
                var penalty : Long = 0L
                for(i in 0 until cur_year_rprt_cnt) penalty += business_profit_amount[i*2-2]?:throw IllegalArgumentException("business_profit_amount[${i*2-2}] is null")
                var percentile = if(penalty < 0) 6 * cur_year_rprt_cnt else 0
                if((business_profit_amount[14]?:throw IllegalArgumentException("-1 year business_profit_amount is null"))<0) {
                    if ((business_profit_amount[22]
                                    ?: throw IllegalArgumentException("-2 year business_profit_amount is null")) < 0) {
                        if ((business_profit_amount[30]
                                        ?: throw IllegalArgumentException("-3 year business_profit_amount is null")) < 0) {
                            if ((business_profit_amount[38]
                                            ?: throw IllegalArgumentException("-4 year business_profit_amount is null")) < 0) {
                                val sp_score = if (penalty < 0) 25 * cur_year_rprt_cnt else 0
                                println("상폐 조건 ${sp_score}% 충족")
                            } else {
                                percentile += 75
                                println("상폐 조건 ${percentile}% 충족")
                            }
                        } else {
                            percentile += 50
                            println("상폐 조건 ${percentile}% 충족")
                        }
                    } else {
                        percentile += 25
                        println("상폐 조건 ${percentile}% 충족")
                    }
                } else {
                    if (percentile == 0) println("해당 사항 없음")
                    else println("상폐 조건 ${percentile}% 충족")
                }
            }
        }
        else println("코스피는 해당 사항 없음")
    }

    //함수 4: 자본 잠식
    fun test_capital_erosion() : Unit{
        fun capital_erosion_rate(index : Int) : Double{
            return capital_amount[index]?.let{(it-(ownership_amount[index]?.toDouble()?:throw IllegalArgumentException("ownership_amount[$index] is null")))/it * 100}
                    ?:throw IllegalArgumentException("capital_amount[$index] is null")
        }
        when(corp_cls){
            "K"->{
                when(cur_year_rprt_cnt) {
                    0 -> when(last_year_rprt_cnt){
                             4 -> {
                                if(capital_erosion_rate(14)>=capital_erosion_rate_limit) println("관리 A 100%") // null checked.
                                else println("관리 A 0%")
                                if(ownership_amount[14]!! < capital_erosion_limit) println("관리 B 100%")
                                else println("관리 B 0%")
                            }
                            else ->{
                                if(capital_erosion_rate(12)>=capital_erosion_rate_limit) println("관리 A 50%") // null checked.
                                else println("관리 A 0%")
                                if(ownership_amount[12]!! < capital_erosion_limit.toDouble()/2) println("관리 B 50%")
                                else println("관리 B 0%")
                            }
                        }
                    1 -> {
                        when{
                            (capital_erosion_rate(14)>=capital_erosion_rate_limit) and (capital_erosion_rate(0)>=capital_erosion_rate_limit) -> println("상폐 A 50%")
                            capital_erosion_rate(14)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            capital_erosion_rate(0)>=capital_erosion_rate_limit -> println("관리 A 50%")
                            else -> println("관리 A 0%")

                        }
                        when{
                            (ownership_amount[14]!! < capital_erosion_limit) and (ownership_amount[0]!! < capital_erosion_limit.toDouble()/2) -> println("상폐 B 50%")
                            ownership_amount[14]!! < capital_erosion_limit -> {
                                println("관리 B 100%")
                                println("상폐 B 0%")
                            }
                            ownership_amount[0]!! < capital_erosion_limit.toDouble()/2 -> println("관리 B 50%")
                            else -> println("관리 B 0%")
                        }
                    }
                    2 -> {
                        when{
                            (capital_erosion_rate(14)>=capital_erosion_rate_limit) and (capital_erosion_rate(2)>=capital_erosion_rate_limit) -> println("상폐 A 100%")
                            capital_erosion_rate(14)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            capital_erosion_rate(2)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            else -> println("관리 A 0%")

                        }
                        when{
                            (ownership_amount[14]!! < capital_erosion_limit) and (ownership_amount[2]!! < capital_erosion_limit) -> println("상폐 B 100%")
                            ownership_amount[14]!! < capital_erosion_limit -> {
                                println("관리 B 100%")
                                println("상폐 B 0%")
                            }
                            ownership_amount[2]!! < capital_erosion_limit -> println("관리 B 100%")
                            else -> println("관리 B 0%")
                        }

                    }
                    3 -> {
                        when{
                            (capital_erosion_rate(2)>=capital_erosion_rate_limit) and (capital_erosion_rate(4)>=capital_erosion_rate_limit) -> println("상폐 A 50%")
                            capital_erosion_rate(2)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            capital_erosion_rate(4)>=capital_erosion_rate_limit -> println("관리 A 50%")
                            else -> println("관리 A 0%")

                        }
                        when{
                            (ownership_amount[2]!! < capital_erosion_limit) and (ownership_amount[4]!! < capital_erosion_limit) -> println("상폐 B 50%")
                            ownership_amount[2]!! < capital_erosion_limit -> {
                                println("관리 B 100%")
                                println("상폐 B 0%")
                            }
                            ownership_amount[5]!! < capital_erosion_limit.toDouble()/2 -> println("관리 B 50%")
                            else -> println("관리 B 0%")
                        }
                    }
                }
            }
            "Y" ->{
                when(cur_year_rprt_cnt){
                    0->{
                        when(last_year_rprt_cnt){
                            4 -> {
                                if(capital_erosion_rate(14)>=capital_erosion_rate_limit) println("관리 A 100%") // null checked.
                                else println("관리 A 0%")
                            }
                            else -> {
                                if(capital_erosion_rate(12)>=capital_erosion_rate_limit) println("관리 A 50%") // null checked.
                                else println("관리 A 0%")
                            }
                        }
                    }
                    1->{
                        when{
                            (capital_erosion_rate(14)>=capital_erosion_rate_limit) and (capital_erosion_rate(0)>=capital_erosion_rate_limit) -> println("상폐 A 25%")
                            capital_erosion_rate(14)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            capital_erosion_rate(0)>=50 -> println("관리 A 25%")
                            else -> println("관리 A 0%")
                        }
                    }
                    2->{
                        when{
                            (capital_erosion_rate(14)>=capital_erosion_rate_limit) and (capital_erosion_rate(2)>=capital_erosion_rate_limit) -> println("상폐 A 50%")
                            capital_erosion_rate(14)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            capital_erosion_rate(2)>=capital_erosion_rate_limit -> {
                                println("관리 A 50%")
                                println("상폐 A 0%")
                            }
                            else -> println("관리 A 0%")
                        }
                    }
                    3->{
                        when{
                            (capital_erosion_rate(14)>=capital_erosion_rate_limit) and (capital_erosion_rate(4)>=capital_erosion_rate_limit) -> println("상폐 A 75%")
                            capital_erosion_rate(14)>=capital_erosion_rate_limit -> {
                                println("관리 A 100%")
                                println("상폐 A 0%")
                            }
                            capital_erosion_rate(4)>=capital_erosion_rate_limit -> println("관리 A 75%")
                            else -> println("관리 A 0%")
                        }
                    }
                }
            }
            else -> throw IllegalArgumentException("Unidentified market label $corp_cls")
        }
        println("작년자본잠식률 : ${capital_erosion_rate(14)}")
        println("올해 1분기 자본잠식률 : ${capital_erosion_rate(0)}")
        println("올해 반기 자본잠식률 : ${capital_erosion_rate(2)}")

        //자기자본 = 자산총계
        println("작년 자기자본 : ${total_assets_amount[14]}")
        println("올해 1분기 자기자본 : ${total_assets_amount[0]}")
        println("올해 자기자본 : ${total_assets_amount[2]}")
    }
}


