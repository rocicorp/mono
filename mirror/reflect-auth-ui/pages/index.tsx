import 'firebase/auth';
import {StyledFirebaseAuth} from '../components/StyledFirebaseAuth';
import {auth, uiConfig} from '../config/firebaseAuthUI.config';

export default function Home() {
  return <StyledFirebaseAuth uiConfig={uiConfig} firebaseAuth={auth} />;
}
