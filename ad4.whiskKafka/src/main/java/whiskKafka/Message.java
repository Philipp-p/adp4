package whiskKafka;

public class Message {
    final int id;
    final long timeStamp;
    final double value;

    public Message(int id) {
        this.id = id;
        this.timeStamp = System.currentTimeMillis();
        this.value = Math.random() * 40;

    }
}
