import React, { useState } from 'react';
import { View, Text, Button, Alert, StyleSheet } from 'react-native';
import { Stack } from 'expo-router';

// A simple screen that emulates similar functionality to hello-zero's App.tsx
export default function HomeScreen() {
  // Example state for messages (replace with your actual data fetching logic)
  const [messageCount, setMessageCount] = useState<number>(0);

  // Handle a sample action, e.g., adding a message
  const handleAddMessage = () => {
    setMessageCount((prevCount) => prevCount + 1);
    Alert.alert('Message Added', `Total messages: ${messageCount + 1}`);
  };

  return (
    <View style={styles.container}>
      <Text style={styles.title}>Hello Zero Expo</Text>
      <Text style={styles.message}>
        {messageCount === 0 ? 'No messages yet' : `Total messages: ${messageCount}`}
      </Text>
      <Button title="Add Message" onPress={handleAddMessage} />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, alignItems: 'center', justifyContent: 'center', padding: 16 },
  title: { fontSize: 22, fontWeight: 'bold', marginBottom: 12 },
  message: { fontSize: 16, marginVertical: 8 },
});
