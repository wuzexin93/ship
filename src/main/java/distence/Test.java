package distence;

import java.math.BigDecimal;
import java.util.HashSet;

public class Test {
    public static void main(String[] args) {
        String[] arr1 = "1.3885066e+09   1.5987819e+07   7.2989764e+07   2.5562800e+05   2.6646365e+01   1.2164961e+02".split("\\s+");
        String[] arr2 = "1.3885057e+09   1.7117823e+07   7.3412519e+07   2.6046400e+05   2.8529705e+01   1.2235420e+02".split("\\s+");

        Long a = Math.round(Double.parseDouble(new BigDecimal(arr1[3]).toPlainString()));
        Long b = Math.round(Double.parseDouble(new BigDecimal(arr2[3]).toPlainString()));

        PairWritable pairWritable1 = new PairWritable(a, b);
        PairWritable pairWritable2 = new PairWritable(b, a);

        System.out.println(pairWritable1.equals(pairWritable2));

        HashSet<PairWritable> set = new HashSet<>();
        set.add(pairWritable1);
        set.add(pairWritable2);

        System.out.println(set.size());

        for (PairWritable writable : set) {
            System.out.println(writable.toString());
        }

//        Long a = Long.valueOf(1);
//        Long b = Long.valueOf(2222);
//
//        System.out.println(a.hashCode() + " " + b.hashCode());
//
//        System.out.println(5 << 1 & 7);
//        System.out.println(7 << 1 & 5);
    }
}
