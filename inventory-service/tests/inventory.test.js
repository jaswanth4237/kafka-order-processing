describe('Inventory Logic', () => {
    it('should correctly identify stock availability', () => {
        const stock = [
            { sku: 'PROD-001', quantity: 10 },
            { sku: 'PROD-002', quantity: 5 },
        ];

        const checkStock = (items) => {
            for (const item of items) {
                const found = stock.find((s) => s.sku === item.sku);
                if (!found || found.quantity < item.quantity) return false;
            }
            return true;
        };

        expect(checkStock([{ sku: 'PROD-001', quantity: 1 }])).toBe(true);
        expect(checkStock([{ sku: 'PROD-001', quantity: 11 }])).toBe(false);
        expect(checkStock([{ sku: 'PROD-003', quantity: 1 }])).toBe(false);
    });
});
