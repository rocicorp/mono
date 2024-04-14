import classNames from 'classnames';
import SignalUrgentIcon from './assets/icons/claim.svg';
import SignalNoPriorityIcon from './assets/icons/dots.svg';
import SignalMediumIcon from './assets/icons/signal-medium.svg';
import SignalStrongIcon from './assets/icons/signal-strong.svg';
import SignalWeakIcon from './assets/icons/signal-weak.svg';
import {Priority} from './issue';

interface Props {
  priority: Priority;
  className?: string;
}

const ICONS = {
  [Priority.High]: SignalStrongIcon,
  [Priority.Medium]: SignalMediumIcon,
  [Priority.Low]: SignalWeakIcon,
  [Priority.Urgent]: SignalUrgentIcon,
  [Priority.None]: SignalNoPriorityIcon,
};

export default function PriorityIcon({priority, className}: Props) {
  const classes = classNames('w-3.5 h-3.5 rounded', className);

  // eslint-disable-next-line @typescript-eslint/naming-convention
  const Icon = ICONS[priority];

  return <Icon className={classes} />;
}
