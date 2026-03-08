SELECT
    c.customerID AS customer_id,
    t.*
FROM samples.bakehouse.sales_customers c
left join samples.bakehouse.sales_transactions t on t.customerID = c.customerID
JOIN samples.bakehouse.sales_franchises     o ON t.franchiseID = o.franchiseID
LEFT JOIN samples.bakehouse.sales_suppliers a ON o.supplierID   = a.supplierID


