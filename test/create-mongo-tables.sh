#!/bin/bash
# Script to create test MongoDB database with sample data
# This sets up a MongoDB database similar to how duckdb-postgres sets up Postgres

set -e

MONGO_HOST=${MONGO_HOST:-localhost}
MONGO_PORT=${MONGO_PORT:-27017}
MONGO_DB=${MONGO_DB:-duckdb_mongo_test}

echo "Creating test MongoDB database: $MONGO_DB"

# Check if mongosh is available
if ! command -v mongosh &> /dev/null; then
    echo "Error: mongosh is not installed. Please install MongoDB shell."
    echo "On macOS: brew install mongosh"
    exit 1
fi

# Create test database and collections with sample data
mongosh "mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB" --eval "
// Drop database if it exists
db.dropDatabase();

// Create users collection with various data types
db.users.insertMany([
  {
    _id: ObjectId('507f1f77bcf86cd799439011'),
    name: 'Alice',
    email: 'alice@example.com',
    age: 30,
    active: true,
    balance: 1000.50,
    tags: ['admin', 'user'],
    address: {
      street: '123 Main St',
      city: 'New York',
      zip: '10001',
      country: 'USA'
    },
    created_at: new Date('2023-01-01T00:00:00Z')
  },
  {
    _id: ObjectId('507f1f77bcf86cd799439012'),
    name: 'Bob',
    email: 'bob@example.com',
    age: 25,
    active: false,
    balance: 500.25,
    tags: ['user'],
    address: {
      street: '456 Oak Ave',
      city: 'Los Angeles',
      zip: '90001',
      country: 'USA'
    },
    created_at: new Date('2023-02-01T00:00:00Z')
  },
  {
    _id: ObjectId('507f1f77bcf86cd799439013'),
    name: 'Charlie',
    email: 'charlie@example.com',
    age: 35,
    active: true,
    balance: 2000.75,
    tags: ['admin', 'premium'],
    address: {
      street: '789 Pine Rd',
      city: 'Chicago',
      zip: '60601',
      country: 'USA'
    },
    created_at: new Date('2023-03-01T00:00:00Z')
  },
  {
    name: 'Diana',
    email: 'diana@example.com',
    age: 28,
    active: true,
    balance: 750.00,
    tags: ['user'],
    address: {
      street: '321 Elm St',
      city: 'Houston',
      zip: '77001',
      country: 'USA'
    },
    created_at: new Date('2023-04-01T00:00:00Z')
  }
]);

// Create products collection
db.products.insertMany([
  {
    name: 'Laptop',
    category: 'Electronics',
    price: 999.99,
    in_stock: true,
    quantity: 50,
    specs: {
      cpu: 'Intel i7',
      ram: '16GB',
      storage: '512GB SSD'
    },
    tags: ['computer', 'electronics', 'laptop']
  },
  {
    name: 'Mouse',
    category: 'Electronics',
    price: 29.99,
    in_stock: true,
    quantity: 200,
    specs: {
      type: 'Wireless',
      dpi: 1600
    },
    tags: ['computer', 'accessories']
  },
  {
    name: 'Desk',
    category: 'Furniture',
    price: 299.99,
    in_stock: false,
    quantity: 0,
    specs: {
      material: 'Wood',
      dimensions: {
        width: 120,
        height: 75,
        depth: 60
      }
    },
    tags: ['furniture', 'office']
  }
]);

// Create matrix collection with arrays of arrays
db.matrix.insertMany([
  {
    _id: 'MAT-001',
    name: '2D Matrix',
    data: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
  },
  {
    _id: 'MAT-002',
    name: '3D Matrix',
    data: [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
  },
  {
    _id: 'MAT-003',
    name: 'Mixed Matrix',
    data: [[10, 20], [30, 40], [50, 60]]
  }
]);

// Create orders collection with nested arrays
db.orders.insertMany([
  {
    order_id: 'ORD-001',
    customer_id: ObjectId('507f1f77bcf86cd799439011'),
    items: [
      { product: 'Laptop', quantity: 1, price: 999.99 },
      { product: 'Mouse', quantity: 2, price: 29.99 }
    ],
    total: 1059.97,
    status: 'completed',
    order_date: new Date('2023-05-01T00:00:00Z')
  },
  {
    order_id: 'ORD-002',
    customer_id: ObjectId('507f1f77bcf86cd799439012'),
    items: [
      { product: 'Desk', quantity: 1, price: 299.99 }
    ],
    total: 299.99,
    status: 'pending',
    order_date: new Date('2023-05-02T00:00:00Z')
  },
  {
    order_id: 'ORD-003',
    customer_id: ObjectId('507f1f77bcf86cd799439013'),
    items: [],
    total: 0,
    status: 'cancelled',
    order_date: new Date('2023-05-03T00:00:00Z')
  },
  {
    order_id: 'ORD-004',
    customer_id: ObjectId('507f1f77bcf86cd799439011'),
    items: [
      { product: 'Keyboard', quantity: 1 }
    ],
    notes: ['urgent', 'gift'],
    total: 79.99,
    status: 'pending',
    order_date: new Date('2023-05-04T00:00:00Z')
  }
]);

// Create empty collection for testing (just create the collection, don't insert empty array)
db.createCollection('empty_collection');

// Create collection with type conflicts (same field, different types)
db.type_conflicts.insertMany([
  { id: '123', value: 'string' },
  { id: 456, value: 789 },
  { id: true, value: false }
]);

// Create collection with deeply nested documents
db.deeply_nested.insertMany([
  {
    level1: {
      level2: {
        level3: {
          level4: {
            level5: {
              level6: {
                value: 'deep value'
              }
            }
          }
        }
      }
    }
  }
]);

// Create collection with nested object scalar fields
db.nested_scalars_test.insertMany([
  {
    name: 'Document1',
    Parent: {
      Object: {
        Child: {
          String: 'test_value',
          Int: 42,
          Bool: true,
          Date: new Date('2023-01-01T00:00:00Z')
        }
      }
    }
  },
  {
    name: 'Document2',
    Level1: {
      Level2: {
        Level3: {
          Value: 'nested_value',
          Number: 100
        }
      }
    }
  },
  {
    name: 'Document3',
    Parent: {
      Object: {
        Child: {
          String: 'another_value',
          Int: 100,
          Bool: false,
          Date: new Date('2023-02-01T00:00:00Z'),
          OptionalField: 'optional_value'
        }
      }
    }
  },
  {
    name: 'Document4',
    Parent: {
      Object: {
        Child: {
          String: null,
          Int: 0,
          Bool: false
        }
      }
    }
  }
]);

print('Test database created successfully!');
print('Database: ' + db.getName());
print('Collections: ' + db.getCollectionNames().join(', '));
"

echo ""
echo "Test MongoDB database '$MONGO_DB' created successfully!"
echo "Collections: users, products, orders, empty_collection, type_conflicts, deeply_nested, nested_scalars_test"
echo ""

# Export environment variables for tests
export MONGODB_TEST_DATABASE_AVAILABLE=1
export MONGO_TEST_CONNECTION="mongodb://$MONGO_HOST:$MONGO_PORT/$MONGO_DB"

echo "Environment variables set:"
echo "  MONGODB_TEST_DATABASE_AVAILABLE=1"
echo "  MONGO_TEST_CONNECTION='$MONGO_TEST_CONNECTION'"
echo ""
echo "Note: If you run this script directly (not sourced), these variables will only be"
echo "available in the current shell session. To make them available in your shell, run:"
echo "  source test/create-mongo-tables.sh"
echo "or"
echo "  . test/create-mongo-tables.sh"

