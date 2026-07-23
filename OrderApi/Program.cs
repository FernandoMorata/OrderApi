using Confluent.Kafka;
using OrderApi;
using System.ComponentModel.DataAnnotations;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// 1. Configura e registra o Producer do Kafka no DI Container
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092" // Substitua pelo endereço do seu broker Kafka
};

builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    return new ProducerBuilder<string, string>(producerConfig).Build();
});
// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Registra o Consumer para rodar como Background Worker junto com a API
builder.Services.AddHostedService<OrderCreatedConsumer>();

var app = builder.Build();



// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// Banco em memória pra simular os pedidos salvos
var orderDatabase = new List<Order>();

// Endpoint POST para criar o pedido
app.MapPost("/api/orders", async (CreateOrderRequest request, IProducer<string, string> producer) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerName) || request.Amount <= 0)
    {
        return Results.BadRequest(new { Message = "Dados do pedido inválidos." });
    }

    var order = new Order
    (
        Id: Guid.NewGuid(),
        CustomerName: request.CustomerName,
        Amount: request.Amount,
        CreatedAt: DateTime.UtcNow,
        Status: "created"
    );
    orderDatabase.Add(order);

    // Serializa o pedido para JSON e envia ao Kafka
    var messageJson = JsonSerializer.Serialize(order);
    var kafkaMessage = new Message<string, string>
    {
        Key = order.Id.ToString(),// Chave para garantir ordenação por ID se necessário
        Value = messageJson
    };
    //publica a mensagem no tópico "order-created"
    await producer.ProduceAsync("order-created", kafkaMessage);
    return Results.Created($"/api/orders/{order.Id}", order);
});

// Endpoint GET para consultar os pedidos
app.MapGet($"/api/orders",() => Results.Ok(orderDatabase));

app.UseAuthorization();

app.MapControllers();

app.Run();

// DTOs
public record CreateOrderRequest(
    [Required] string CustomerName,
    [Required] decimal Amount
);

public record Order(
    Guid Id,
    string CustomerName,
    decimal Amount,
    DateTime CreatedAt,
    string Status
);
