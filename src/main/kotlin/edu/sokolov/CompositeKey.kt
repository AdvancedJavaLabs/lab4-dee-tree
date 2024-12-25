package edu.sokolov

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput
import java.io.DataOutput
import java.util.Objects

class CompositeKey(
    val category: Text,
    val revenue: DoubleWritable,
    val quantity: LongWritable
) : WritableComparable<CompositeKey> {

    constructor() : this(category = Text(), revenue = DoubleWritable(), quantity = LongWritable())

    override fun write(dest: DataOutput) {
        category.write(dest)
        revenue.write(dest)
        quantity.write(dest)
    }

    override fun readFields(src: DataInput) {
        category.readFields(src)
        revenue.readFields(src)
        quantity.readFields(src)
    }

    override fun compareTo(other: CompositeKey): Int {
        return other.revenue.compareTo(revenue)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return false
        if (other == null || javaClass != other.javaClass) return false
        other as CompositeKey
        if (category != other.category) return false
        if (revenue != other.revenue) return false
        return quantity == other.quantity
    }

    override fun hashCode(): Int {
        return Objects.hash(category, revenue, quantity)
    }

    override fun toString(): String {
        return "${revenue.get().toBigDecimal().toPlainString()},$quantity"
    }
}