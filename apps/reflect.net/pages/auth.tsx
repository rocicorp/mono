import 'firebase/auth';
import {StyledFirebaseAuth} from '@/components/Firebase/StyledFirebaseAuth';
import {auth, uiConfig} from '@/firebase-config/firebase-auth-ui-config';

export default function Auth() {
  return <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={auth} />;
}
