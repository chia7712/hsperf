package com.chia7712.myscala

import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

class TestCellConverter extends AssertionsForJUnit {

  @Test
  def testKeyConversion() = {
    val key = Key(ByteConverter.toBytes(1), ByteConverter.toBytes(2), ByteConverter.toBytes(3))
    var anotherCell = CellConverter.toCell(CellConverter.toDeleteCell(key))
    assertEquals(0, key.compare(anotherCell.key))

    val anotherDelete = CellConverter.toDelete(key)
    anotherDelete.getFamilyCellMap.forEach((k, cells) => {
      assertEquals(0, ByteConverter.compare(k, key.family))
      cells.forEach(c => assertEquals(0, key.compare(CellConverter.toCell(c).key)))
    })

    val cell = Cell(key, ByteConverter.toBytes(4))
    anotherCell = CellConverter.toCell(CellConverter.toPutCell(cell))
    assertEquals(0, cell.compare(anotherCell))

    val anotherPut = CellConverter.toPut(cell)
    anotherPut.getFamilyCellMap.forEach((k, cells) => {
      assertEquals(0, ByteConverter.compare(k, key.family))
      cells.forEach(c => assertEquals(0, cell.compare(CellConverter.toCell(c))))
    })
  }
}
