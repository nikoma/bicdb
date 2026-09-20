use tonic::{Request, Response, Status};

mod wire {
    tonic::include_proto!("bicdb.example.inventory.v1");
}

#[derive(Default)]
struct Inventory;

#[tonic::async_trait]
impl wire::inventory_server::Inventory for Inventory {
    async fn reserve(
        &self,
        request: Request<wire::ReserveRequest>,
    ) -> Result<Response<wire::ReserveResponse>, Status> {
        let expected = std::env::var("BICDB_GRPC_EXAMPLE_TOKEN")
            .unwrap_or_else(|_| "operator-secret".to_string());
        let authorization = format!("Bearer {expected}");
        if request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(authorization.as_str())
            || request.metadata().get("x-bicdb-trace-id").is_none()
        {
            return Err(Status::unauthenticated("missing provider metadata"));
        }
        let request = request.into_inner();
        let accepted = !request.lines.is_empty()
            && request
                .lines
                .iter()
                .all(|line| !line.sku.is_empty() && line.quantity > 0);
        Ok(Response::new(wire::ReserveResponse {
            accepted,
            reservation_id: request
                .request_id
                .unwrap_or_else(|| "generated-reservation".to_string()),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("BICDB_GRPC_EXAMPLE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:50051".to_string())
        .parse()?;
    tonic::transport::Server::builder()
        .add_service(wire::inventory_server::InventoryServer::new(Inventory))
        .serve(address)
        .await?;
    Ok(())
}
