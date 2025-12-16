import {
  arrow,
  autoUpdate,
  flip,
  FloatingArrow,
  FloatingPortal,
  offset,
  shift,
  useDelayGroup,
  useDismiss,
  useFloating,
  useFocus,
  useHover,
  useInteractions,
  useMergeRefs,
  useRole,
  useTransitionStatus,
} from '@floating-ui/react';
import classNames from 'classnames';
import {
  createContext,
  forwardRef,
  useContext,
  useMemo,
  useRef,
  useState,
  type HTMLProps,
  type ReactNode,
} from 'react';
import './tooltip.css';

type ContextType = ReturnType<typeof useTooltip> | null;

const TooltipContext = createContext<ContextType>(null);

const useTooltipContext = () => {
  const context = useContext(TooltipContext);
  if (context === null) {
    throw new Error('Tooltip components must be wrapped in <Tooltip />');
  }
  return context;
};

interface TooltipOptions {
  initialOpen?: boolean | undefined;
  open?: boolean | undefined;
  onOpenChange?: ((open: boolean) => void) | undefined;
}

function useTooltip({
  initialOpen = false,
  open: controlledOpen,
  onOpenChange: setControlledOpen,
}: TooltipOptions = {}) {
  const padding = 10;
  const [uncontrolledOpen, setUncontrolledOpen] = useState(initialOpen);
  const open = controlledOpen ?? uncontrolledOpen;
  const setOpen = setControlledOpen ?? setUncontrolledOpen;
  const arrowRef = useRef<SVGSVGElement>(null);
  const floatingData = useFloating({
    open: open,
    onOpenChange: setOpen,
    placement: 'top',
    middleware: [
      offset(padding),
      flip(),
      shift(),
      arrow({
        element: arrowRef,
        padding,
      }),
    ],
    whileElementsMounted: autoUpdate,
    transform: false,
  });
  const {context} = floatingData;
  const {isMounted, status} = useTransitionStatus(context);
  const {delay} = useDelayGroup(context);
  const hover = useHover(context, {
    move: false,
    delay,
    enabled: controlledOpen === undefined,
  });
  const focus = useFocus(context, {
    enabled: controlledOpen === undefined,
  });
  const dismiss = useDismiss(context);
  const role = useRole(context, {
    role: 'label',
  });

  const interactions = useInteractions([hover, focus, dismiss, role]);

  return useMemo(
    () => ({
      ...floatingData,
      ...interactions,
      isMounted,
      status,
      arrowRef,
    }),
    [floatingData, interactions, isMounted, status],
  );
}

export function Tooltip({
  children,
  ...options
}: {children: ReactNode} & TooltipOptions) {
  const context = useTooltip(options);
  return (
    <TooltipContext.Provider value={context}>
      {children}
    </TooltipContext.Provider>
  );
}

export const TooltipTrigger = forwardRef<
  HTMLDivElement,
  HTMLProps<HTMLDivElement>
>(({children, ...props}, propRef) => {
  const context = useTooltipContext();
  const ref = useMergeRefs([context.refs.setReference, propRef]);
  return (
    <div ref={ref} {...context.getReferenceProps(props)}>
      {children}
    </div>
  );
});

export const TooltipContent = forwardRef<
  HTMLDivElement,
  HTMLProps<HTMLDivElement>
>(({children, ...props}, propRef) => {
  const context = useTooltipContext();
  const ref = useMergeRefs([context.refs.setFloating, propRef]);
  if (!context.isMounted) {
    return null;
  }
  return (
    <FloatingPortal>
      <div
        ref={ref}
        style={{
          ...context.floatingStyles,
        }}
        {...context.getFloatingProps(props)}
        className={classNames('tooltip-content', props.className)}
        data-placement={context.placement}
        data-status={context.status}
      >
        {children}
        <FloatingArrow
          className="tooltip-arrow"
          ref={context.arrowRef}
          context={context.context}
          strokeWidth={1}
        />
      </div>
    </FloatingPortal>
  );
});
