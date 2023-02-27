import ballerina/log;
import ballerinax/mysql;
import ballerina/http;
import ballerina/sql;
import ballerina/email;

configurable string host = ?;
configurable string username = ?;
configurable string password = ?;
configurable string db = ?;
configurable int port = ?;

configurable string smtpHost = ?;
configurable string smtpUser = ?;
configurable string smtpPassword = ?;
configurable int smtpPort = ?;

type Product record {
    int id?;
    string title;
    string description;
    float price;
};

type Subscriber record {
    int id;
    int productId;
    string email;
};

service / on new http:Listener(9090) {
    private mysql:Client mySqlClient;
    function init() returns error? {
        self.mySqlClient = check new (host, username, password, db, port, connectionPool = {maxOpenConnections: 3});
    }

    resource function get products() returns Product[]|error {
        // TODO add following db params as secrets
        stream<Product, sql:Error?> result = self.mySqlClient->query(`select * from products;`);

        Product[] products = check from Product product in result
            select product;
        log:printInfo("Catalog array: ", catalog = products);
        return products;
    }

    resource function get products/[int id]() returns Product|error {
        // TODO add mysql client as a field in init()
        Product|sql:Error product = self.mySqlClient->queryRow(`select * from products where id = ${id};`);
        if product is Product {
            return product;
        } else {
            log:printError("Error while retrieving cart: ", product);
            return error("Error while retrieving cart");
        }
    }

    resource function post products(@http:Payload Product product) returns error? {
        sql:ExecutionResult|sql:Error result = self.mySqlClient->execute(`insert into products(title,description,price) values(${product.title},${product.description},${product.price})`);
        if result is sql:ExecutionResult {
            log:printInfo("Added product", product = product);
        } else {
            log:printError("Failed to add product", product = product);
            return error("Failed to add product");
        }
    }

    resource function put products(@http:Payload Product product) returns error? {
        sql:ExecutionResult|sql:Error result = self.mySqlClient->execute(`update products set title =  ${product.title}, description = ${product.description}, price = ${product.price} where id = ${product.id}`);
        if result is sql:ExecutionResult {
            log:printInfo("Updated product", product = product);
            check self.sendNotifications(product);
        } else {
            log:printError("Failed to update product", product = product);
            return error("Failed to update product");
        }
    }

    function sendNotifications(Product product) returns error? {
        stream<Subscriber, sql:Error?> result = self.mySqlClient->query(`select * from subscribers where productId = ${product.id}`);
        Subscriber[] subscribers = check from var item in result
            select item;
        foreach Subscriber sub in subscribers {
            email:SmtpClient smtpClient = check new (smtpHost, smtpUser, smtpPassword, port = smtpPort, security = email:START_TLS_AUTO);

            email:Message email = {
                to: sub.email,
                subject: "Product Updated",
                body: string `Product '${product.title}' was updated`
            };

            email:Error? err = smtpClient->sendMessage(email);
            if err is email:Error {
                log:printError("Failed to send email", err);
                return err;
            }
        }
    }
}
