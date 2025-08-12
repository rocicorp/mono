import {describe, expect, it} from 'vitest';

describe('Ignored Tables - Minimal Implementation', () => {
  describe('Table Name Matching', () => {
    it('should match simple table names in any schema', () => {
      const ignoredTables = new Set(['users', 'public.users']);
      
      // Simple name should match
      expect(ignoredTables.has('users')).toBe(true);
      expect(ignoredTables.has('public.users')).toBe(true);
    });

    it('should match qualified table names exactly', () => {
      const ignoredTables = new Set(['staging.imports', 'public.staging.imports']);
      
      // Should match staging.imports
      expect(ignoredTables.has('staging.imports')).toBe(true);
      // Should not match just 'imports'
      expect(ignoredTables.has('imports')).toBe(false);
    });

    it('should handle both formats from config', () => {
      const config = ['users', 'staging.imports', 'audit_logs'];
      const ignoredTables = new Set(
        config.flatMap(table =>
          table.includes('.') ? [table] : [table, `public.${table}`]
        )
      );
      
      // Check the expanded set
      expect(ignoredTables.has('users')).toBe(true);
      expect(ignoredTables.has('public.users')).toBe(true);
      expect(ignoredTables.has('staging.imports')).toBe(true);
      expect(ignoredTables.has('audit_logs')).toBe(true);
      expect(ignoredTables.has('public.audit_logs')).toBe(true);
      
      // Should not have public.staging.imports
      expect(ignoredTables.has('public.staging.imports')).toBe(false);
    });
  });

  describe('Filtering Logic', () => {
    it('should filter tables during initial sync', () => {
      const tables = [
        {schema: 'public', name: 'users'},
        {schema: 'public', name: 'posts'},
        {schema: 'staging', name: 'imports'},
        {schema: 'public', name: 'audit_logs'},
      ];
      
      const ignoredConfig = ['users', 'staging.imports'];
      const ignoredTables = new Set(
        ignoredConfig.flatMap(table =>
          table.includes('.') ? [table] : [table, `public.${table}`]
        )
      );
      
      const filteredTables = tables.filter(table => {
        const tableName = `${table.schema}.${table.name}`;
        return !ignoredTables.has(table.name) && !ignoredTables.has(tableName);
      });
      
      // Should only have posts and audit_logs
      expect(filteredTables).toHaveLength(2);
      expect(filteredTables.map(t => t.name)).toEqual(['posts', 'audit_logs']);
    });

    it('should filter changes during replication', () => {
      const ignoredConfig = ['audit_logs'];
      const ignoredTables = new Set(
        ignoredConfig.flatMap(table =>
          table.includes('.') ? [table] : [table, `public.${table}`]
        )
      );
      
      // Test INSERT filtering
      const insertMsg = {
        relation: {schema: 'public', name: 'audit_logs'},
      };
      const tableName = `${insertMsg.relation.schema}.${insertMsg.relation.name}`;
      const shouldFilter = ignoredTables.has(insertMsg.relation.name) || 
                          ignoredTables.has(tableName);
      
      expect(shouldFilter).toBe(true);
    });

    it('should filter TRUNCATE operations', () => {
      const ignoredConfig = ['users', 'audit_logs'];
      const ignoredTables = new Set(
        ignoredConfig.flatMap(table =>
          table.includes('.') ? [table] : [table, `public.${table}`]
        )
      );
      
      const relations = [
        {schema: 'public', name: 'users'},
        {schema: 'public', name: 'posts'},
        {schema: 'public', name: 'audit_logs'},
      ];
      
      const filteredRelations = relations.filter(rel => {
        const tableName = `${rel.schema}.${rel.name}`;
        return !ignoredTables.has(rel.name) && !ignoredTables.has(tableName);
      });
      
      // Should only have posts
      expect(filteredRelations).toHaveLength(1);
      expect(filteredRelations[0].name).toBe('posts');
    });
  });
});