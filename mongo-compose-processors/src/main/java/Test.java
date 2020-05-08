import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Test {

    public static void main(String[] args) throws Exception{
        JSONObject opt = new JSONObject();
        JSONObject aa = new JSONObject();
        aa.put("aa","a");
        opt.put("opt",aa);

        String content = "{\"ts\": {\"$timestamp\": {\"t\": 1588089601, \"i\": 66}}, \"t\": {\"$numberLong\": \"15\"}, \"h\": {\"$numberLong\": \"3208476680914972232\"}, \"v\": 2, \"op\": \"u\", \"ns\": \"OrderProcessing.trialvouchers\", \"ui\": {\"$binary\": \"WU7EsBB+HqxxivFhQyDkhQ==\", \"$type\": \"03\"}, \"o2\": {\"_id\": {\"$oid\": \"5ea84c271503a32305db3acd\"}}, \"wall\": {\"$date\": 1588089601468}, \"o\": {\"$v\": 1, \"$set\": {\"confirmed\": true, \"updatedAt\": {\"$date\": 1588089601466}}}}";

        String con = "{\"ts\": {\"$timestamp\": {\"t\": 1588089659, \"i\": 172}}, \"t\": {\"$numberLong\": \"15\"}, \"h\": {\"$numberLong\": \"-9018288330546812935\"}, \"v\": 2, \"op\": \"i\", \"ns\": \"OrderProcessing.trialvouchers\", \"ui\": {\"$binary\": \"WU7EsBB+HqxxivFhQyDkhQ==\", \"$type\": \"03\"}, \"wall\": {\"$date\": 1588089659691}, \"lsid\": {\"id\": {\"$binary\": \"rUX6NQzZFOl9llh4WPdMqw==\", \"$type\": \"03\"}, \"uid\": {\"$binary\": \"Y5mrDaxi8gv8RmdTsQ+1j7fmkr7JUsabhNmXAheU0fg=\", \"$type\": \"00\"}}, \"txnNumber\": {\"$numberLong\": \"154\"}, \"stmtId\": 0, \"prevOpTime\": {\"ts\": {\"$timestamp\": {\"t\": 0, \"i\": 0}}, \"t\": {\"$numberLong\": \"-1\"}}, \"o\": {\"_id\": {\"$oid\": \"5ea8533b546a994adb7dd0c3\"}, \"used\": false, \"confirmed\": false, \"userId\": {\"$oid\": \"5c5fe688964fc40e79c47d12\"}, \"content\": {\"kind\": \"时长券\", \"name\": \"初中物理课程体验券\", \"rule\": \"通用型\", \"courseType\": \"publicTextbook\", \"courseId\": {\"stageId\": [\"2\"], \"subjectId\": [\"2\"], \"semesterId\": [], \"publisherId\": []}, \"trialTime\": 604800000, \"reason\": \"转换初中物理体验机会\", \"rangeDescription\": \"初中物理同步提高课通用\"}, \"expiredDate\": {\"$date\": 1590681659676}, \"sendTrialVouchersId\": \"old_to_new_2_2\", \"createdAt\": {\"$date\": 1588089659683}, \"updatedAt\": {\"$date\": 1588089659683}}}";
        BsonDocument currentDoc = BsonDocument.parse(con);
        BsonBinary a = currentDoc.get("ui").asBinary();
        currentDoc.remove("xxx");
        currentDoc.remove("lsid");


        System.out.println(currentDoc);
        System.out.println(a.asUuid(UuidRepresentation.C_SHARP_LEGACY));
    }
}
