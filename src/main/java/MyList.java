import java.util.*;

/**
 * Create By: franksen 2019-05-29 23:03
 */
public class MyList {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<Integer>();

        list.add(1);
        list.add(1);
        list.add(4);

        list.add(2);
        list.add(2);
        list.add(3);



        // 01 storage elements to use new list.  ArrayList 无序
        List<Integer> list1 = new ArrayList<Integer>();

        /*
        for (Integer i: list) {

            //判断新集合中是否包含老集合的元素，如果存在就不插入，否则插入。
            if(!list1.contains(i)){
                list1.add(i);
            }
        }

        for(Integer out: list1){
            System.out.println(out);
        }*/
        //02

       /* for (int i = 0; i < list.size(); i++) {
            Integer s = list.remove(i);
            if(!list1.contains(s)){
                list1.add(i, s);
            }else {
                i--;
            }

        }

        for(Integer out: list1){
            System.out.println(out);
        }*/

       //03 use hashset it can auto drop duplicate elements.
        /*HashSet<Integer> set = new HashSet<Integer>();  //have sort

        for(Integer i : list){
            set.add(i);
        }


        for(Integer out: set){
            System.out.println(out);
        }*/

//        WeakHashMap
    }
}
