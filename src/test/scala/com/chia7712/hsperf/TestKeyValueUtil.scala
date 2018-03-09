package com.chia7712.hsperf

import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class TestKeyValueUtil extends AssertionsForJUnit {

  @Test
  def testKeyConversion() = {
    val key = Key(ByteUtil.toBytes(1), ByteUtil.toBytes(2), ByteUtil.toBytes(3))
    var anotherCell = KeyValueUtil.toCell(KeyValueUtil.toDeleteCell(key))
    assertEquals(0, key.compare(anotherCell.key))

    val anotherDelete = KeyValueUtil.toDelete(key)
    anotherDelete.getFamilyCellMap.forEach((k, cells) => {
      assertEquals(0, ByteUtil.compare(k, key.family))
      cells.forEach(c => assertEquals(0, key.compare(KeyValueUtil.toCell(c).key)))
    })

    val cell = KeyValue(key, ByteUtil.toBytes(4))
    anotherCell = KeyValueUtil.toCell(KeyValueUtil.toPutCell(cell))
    assertEquals(0, cell.compare(anotherCell))

    val anotherPut = KeyValueUtil.toPut(cell)
    anotherPut.getFamilyCellMap.forEach((k, cells) => {
      assertEquals(0, ByteUtil.compare(k, key.family))
      cells.forEach(c => assertEquals(0, cell.compare(KeyValueUtil.toCell(c))))
    })
  }
}
