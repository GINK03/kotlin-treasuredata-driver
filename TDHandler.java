import com.treasuredata.client.*;
import com.google.common.base.Function;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import com.treasuredata.client.model.*;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.List;
import java.util.ArrayList;

class TDHandler {
  public void sample() {
    System.out.println("sample");
  }
  public void sampleHandler(TDClient client, String jobId) {
    client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Object>() {
      @Override
      public Object apply(InputStream input) {
        try { 
					MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
					while(unpacker.hasNext()) {
						// Each row of the query result is array type value (e.g., [1, "name", ...])
						ArrayValue array = unpacker.unpackValue().asArrayValue();
						int id = array.get(0).asIntegerValue().toInt();
            System.out.println("やっほー");
						System.out.println(array);
					}
        } catch (Exception e) {
          System.out.println("内部エラーが発生しました");
        }
        return 0;
			}
    });
  }
  //メモリの使用量にご注意
  public List<ArrayValue> bufferingHandler(TDClient client, String jobId) {
    System.out.println("buffering handlderです。");
    List<ArrayValue> res = new ArrayList<ArrayValue>();
    client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Object>() {
      @Override
      public Object apply(InputStream input) {
        try { 
					MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
					while(unpacker.hasNext()) {
						// Each row of the query result is array type value (e.g., [1, "name", ...])
						ArrayValue array = unpacker.unpackValue().asArrayValue();
            res.add(array);
					}
        } catch (Exception e) {
          System.out.println("内部エラーが発生しました");
          System.out.println(e);
        }
        return 0;
			}
    });
    return res;
  }

  //Denpendency Injection
  public MessageUnpacker unpackerHandler(TDClient client, String jobId) {
    System.out.println("buffering handlderです。");
    ArrayList<MessageUnpacker> res = new ArrayList<MessageUnpacker>();
    MessageUnpacker s = null;
    client.jobResult(jobId, TDResultFormat.MESSAGE_PACK_GZ, new Function<InputStream, Object>() {
      @Override
      public Object apply(InputStream input) {
        try { 
					MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new GZIPInputStream(input));
          res.add(unpacker);
        } catch (Exception e) {
          System.out.println("内部エラーが発生しました");
          System.out.println(e);
        }
        return 0;
			}
    });
    if( res.size() > 0 ) {
      return res.get(0);
    } else {
      return null;
    }
  }

  public void main() { 
    System.out.println("sometime");
  }
}
