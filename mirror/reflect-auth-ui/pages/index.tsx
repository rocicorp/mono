import 'firebase/auth';
import {StyledFirebaseAuth} from '../components/StyledFirebaseAuth';
import {auth, uiConfig} from '../config/firebase-auth-ui-config';

export default function Home() {
  return <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={auth} />;
}
