package ed.inf.adbs.lightdb;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Tuple {
    private final List<Long> data;

//    public Tuple(long [] array) {
//        this.data = Arrays.stream(array).boxed().collect(Collectors.toList()); //converts int[] to ArrayList<Long>
//    }

    public Tuple(List<Long> data){
        this.data = data;
    }

    public long get(int index) {
        return data.get(index);
    }

//    public int size() {
//        return data.size();
//    }

    public List<Long> toList(){
        return new ArrayList<>(data); // to ensure the tuple isn't modified
    }

    @Override
    public String toString() {
        return data.stream().map(String::valueOf)
                .collect(Collectors.joining(","));
    }
}
