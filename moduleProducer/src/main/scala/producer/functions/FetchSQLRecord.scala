package producer.functions

import scalikejdbc._
import system._


object FetchSQLRecord {

  def findById(id: Long)(implicit s: DBSession = AutoSession): List[ExpediaRecord] = {
    sql"select * from expedia where id = $id".map(rs => ExpediaRecord(rs)).list().apply()
  }

}
