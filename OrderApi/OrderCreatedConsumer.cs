using System.Text.Json;
using Confluent.Kafka;

namespace OrderApi
{
    public class OrderCreatedConsumer : BackgroundService
    {
        private readonly ILogger<OrderCreatedConsumer> _logger;
        private readonly ConsumerConfig _consumerConfig;

        public OrderCreatedConsumer(ILogger<OrderCreatedConsumer>Logger)
        {
            _logger = Logger;

            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "order-processing-group",// Identifica o grupo de consumidores
                AutoOffsetReset = AutoOffsetReset.Earliest,// Lê desde as mensagens mais antigas caso não haja checkpoint
                EnableAutoCommit = true
            };
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Yield();

            using var consumer = new ConsumerBuilder<string, string>(_consumerConfig).Build();

            // Inscreve o consumidor no tópico 'order-created'
            consumer.Subscribe("order-created");

            _logger.LogInformation("Consumer do Kafka iniciado. Escutando o tópico 'order-created'...");
            try
            {
                // Aguarda a chegada de uma nova mensagem no tópico
                var consumeResult = consumer.Consume(stoppingToken);

                if (consumeResult?.Message != null)
                {
                    var orderJson = consumeResult.Message.Value;
                    _logger.LogInformation("[Mensagem Recebida] Tópico: {Topic} | Chave: {Key}",
                    consumeResult.Topic, consumeResult.Message.Key);

                    // Simula o processamento do evento (ex: enviar e-mail, gerar nota fiscal, etc.)
                    ProcessOrder(orderJson);

                }
            }
            catch(ConsumeException ex)
            {
                // Executado quando a aplicação é encerrada graciosamente
                consumer.Close();
                _logger.LogInformation("Consumer do Kafka finalizado.");
            }
        }

        private void ProcessOrder(string orderJson)
        {
            try
            {
                using var document = JsonDocument.Parse(orderJson);
                var root = document.RootElement;

                var id = root.GetProperty("id").GetString();
                var customerName = root.GetProperty("customerName").GetString();
                var amount = root.GetProperty("amount").GetDecimal();

                _logger.LogInformation("⚡ [Processando Pedido] ID: {Id} | Cliente: {Customer} | Valor: R$ {Amount}",
                    id, customerName, amount);


            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Erro ao desserializar JSON do pedido.");
            }
        }
    }
}
