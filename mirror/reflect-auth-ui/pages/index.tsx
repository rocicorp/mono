import {StyledFirebaseAuth} from '@/components/StyledFirebaseAuth';
import 'firebase/auth';
import {firebaseConfig} from '@/config/firebaseApp.config';
import {uiConfig} from '@/config/firebaseAuthUI.config';

export default function Home() {
  return (
    <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={firebaseConfig} />
  );
}
