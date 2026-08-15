/**
 * The app's UI primitives, in one import.
 *
 *   import { Button, IconButton, TextField, Badge } from '../ui';
 *
 * Icons are deliberately *not* re-exported here. There are 118 of them and they
 * shadow common words (`File`, `List`, `Filter`, `Search`, `Square`), so folding
 * them into this barrel would make every wildcard import ambiguous and every
 * name collision silent. Import them from `../ui/icons` instead — which is also
 * the single-line change that moves a file off lucide.
 */

export { Badge, type BadgeProps, type BadgeSize, type BadgeTone } from './Badge';
export {
  Button,
  type ButtonProps,
  type ButtonSize,
  type ButtonVariant,
} from './Button';
export { Checkbox, type CheckboxProps } from './Checkbox';
export { Drawer } from './Drawer';
export {
  IconButton,
  type IconButtonProps,
  type IconButtonSize,
  type IconButtonVariant,
} from './IconButton';
export { Modal, type ModalProps, type ModalSize } from './Modal';
export { isModalOpen, modalStack } from './modalStack';
export { PageHeader } from './PageHeader';
export { PaneToggle } from './PaneToggle';
export { Select, type SelectOption, type SelectProps } from './Select';
export { cx, type FieldSize } from './styles';
export {
  TextArea,
  type TextAreaProps,
  TextField,
  type TextFieldProps,
} from './TextField';
export { Tooltip } from './Tooltip';
