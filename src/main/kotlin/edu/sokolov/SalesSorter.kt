package edu.sokolov

import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import java.io.DataInput
import java.io.DataOutput
import java.util.Objects
import java.util.StringTokenizer

class SalesSorter {
    class LinesHandler : Mapper<Any, Text, SalesData, Text>() {

        private val line = SalesData()
        private val empty = Text()

        override fun map(key: Any, value: Text, context: Context) {
            val tokenizer = StringTokenizer(value.toString(), "\n")

            while (tokenizer.hasMoreTokens()) {
                val category = tokenizer.nextToken(",")
                val revenue = tokenizer.nextToken(",").toDoubleOrNull() ?: return
                val quantity = tokenizer.nextToken().toLongOrNull() ?: return

                line.category.set(category)
                line.quantity.set(quantity)
                line.revenue.set(revenue)
                context.write(line, empty)
            }

        }
    }

    class SalesReducer : Reducer<SalesData, Text, SalesData, Text>() {
        override fun reduce(key: SalesData, values: MutableIterable<Text>, context: Context) {
            for (value in values) {
                context.write(key, value)
            }
        }
    }
}


class SalesData(
    val category: Text,
    val revenue: DoubleWritable,
    val quantity: LongWritable
) : WritableComparable<SalesData> {

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

    override fun compareTo(other: SalesData): Int {
        return other.revenue.compareTo(revenue)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return false
        if (other == null || javaClass != other.javaClass) return false
        other as SalesData
        if (category != other.category) return false
        if (revenue != other.revenue) return false
        return quantity == other.quantity
    }

    override fun hashCode(): Int {
        return Objects.hash(category, revenue, quantity)
    }

    override fun toString(): String {
        return "$category\t${revenue.get().toBigDecimal().toPlainString()}\t$quantity"
    }
}