import type { ObjectId } from 'mongodb';

export interface OrderDoc {
  _id: ObjectId;
  customerId: string;
  items: { sku: string; qty: number }[];
  status: 'pending' | 'confirmed' | 'shipped' | 'cancelled';
  createdBy: string; // Auth0 sub
  createdAt: Date;
  updatedAt: Date;
  version: number;
}
