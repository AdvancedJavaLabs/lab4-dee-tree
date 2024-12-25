package edu.sokolov

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import java.util.StringTokenizer


class SalesBook {
    class LinesHandler : Mapper<Any, Text, Text, CompositeKey>() {

        private val line = CompositeKey()

        override fun map(key: Any, value: Text, context: Context) {
            val tokenizer = StringTokenizer(value.toString(), "\n")

            while (tokenizer.hasMoreTokens()) {
                val tid = tokenizer.nextToken(",")
                val pid = tokenizer.nextToken(",")
                val category = tokenizer.nextToken(",")
                val price = tokenizer.nextToken(",").toDoubleOrNull() ?: return
                val quantity = tokenizer.nextToken().toLongOrNull() ?: return

                line.category.set(category)
                line.quantity.set(quantity)
                line.revenue.set(price)

                context.write(line.category, line)
            }
        }
    }

    class SalesReducer : Reducer<Text, CompositeKey, Text, CompositeKey>() {
        private val result = CompositeKey()

        override fun reduce(key: Text, values: MutableIterable<CompositeKey>, context: Context) {
            var quantity = 0L
            var revenue = 0.0
            for (position in values) {
                quantity += position.quantity.get()
                revenue += position.revenue.get()
            }
            result.category.set(key)
            result.quantity.set(quantity)
            result.revenue.set(revenue)

            context.write(key, result)
        }
    }
}
