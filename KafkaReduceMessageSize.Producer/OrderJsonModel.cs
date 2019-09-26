namespace KafkaReduceMessageSize.Producer
{
    public class OrderJsonModel
    {
        public string Id { get; set; }
        public int Status { get; set; }
        public long CreationTime { get; set; }
        public int ProductId { get; set; }
        public int CustomerId { get; set; }
    }
}