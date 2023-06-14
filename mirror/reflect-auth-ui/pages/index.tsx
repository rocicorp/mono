import {StyledFirebaseAuth} from '@/components/StyledFirebaseAuth';
import 'firebase/auth';
import {uiConfig, auth} from '@/config/firebaseAuthUI.config';

export default function Home() {
  return <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={auth} />;
}
