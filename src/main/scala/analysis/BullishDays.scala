package com.example.analysis

import org.apache.spark.sql.DataFrame

object BullishDays {
  def compute(df: DataFrame): Unit = {
    val bullishDays = df.filter($"Close" > $"Open").count()
    println(s"=== Nombre de jours haussiers (Close > Open) : $bullishDays")
  }
}