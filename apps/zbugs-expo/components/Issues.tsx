import { useQuery, useZero } from '@rocicorp/zero/expo';
import React from 'react';
import { View, Text, FlatList, StyleSheet } from 'react-native';

interface Issue {
  id: string;
  title: string;
  open: boolean;
  modified: number;
  created: number;
  creatorID: string | null;
  assigneeID: string | null;
  description: string | null;
}

// Issues component to display a list of issues, similar to list-page.tsx
export default function Issues() {
  const z = useZero();
  const [issues, issuesResult] = useQuery<Issue>(
    z.query.issue.orderBy('modified', 'desc').orderBy('id', 'desc')
  );

  const renderItem = ({ item }: { item: Issue }) => (
    <View style={styles.issueItem}>
      <Text style={styles.issueTitle}>{item.title}</Text>
    </View>
  );

  return (
    <View style={styles.container}>
      <Text style={styles.header}>Issues</Text>
      {issuesResult.isLoading ? (
        <Text>Loading issues...</Text>
      ) : issuesResult.isError || !issues ? (
        <Text>Error loading issues.</Text>
      ) : (
        <FlatList data={issues} renderItem={renderItem} keyExtractor={(item) => item.id} />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 16 },
  header: { fontSize: 24, fontWeight: 'bold', marginBottom: 16 },
  issueItem: { padding: 16, borderBottomWidth: 1, borderBottomColor: '#ddd' },
  issueTitle: { fontSize: 16 },
});
