import org.json.simple.JSONObject;

public class Test {

    public static void main(String[] args) {
        JSONObject opt = new JSONObject();
        JSONObject aa = new JSONObject();
        aa.put("aa","a");
        opt.put("opt",aa);

        System.out.println(opt);
    }
}
