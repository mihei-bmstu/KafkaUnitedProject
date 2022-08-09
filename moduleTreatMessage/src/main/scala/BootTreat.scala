import system._
import treat.functions.AggregateData

object BootTreat {
  def main(args: Array[String]): Unit = {
    println("Start TreatMessage")
    AggregateData.aggregateData(LoadTable.load(Properties.tableMessages))
  }
}
