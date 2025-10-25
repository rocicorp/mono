import type {ProjectRow} from '../../shared/schema.ts';
import projectIcon from '../assets/icons/project-box.svg';
import rocketIcon from '../assets/icons/icon-rocket.svg';
import zeroMarkIcon from '../assets/images/mark.svg';
import {Combobox} from './combobox.tsx';

interface Props {
  onChange: (selectedValue: ProjectRow) => void;
  projects: ProjectRow[];
  selectedProjectName?: string | undefined;
}

function getProjectIcon(project: ProjectRow): string {
  if (project.name === 'Zero') {
    return zeroMarkIcon;
  }
  if (project.name === 'Roci') {
    return rocketIcon;
  }
  return projectIcon;
}

export function ProjectPicker({
  projects,
  selectedProjectName,
  onChange,
}: Props) {
  const lowerCaseName = selectedProjectName?.toLocaleLowerCase();
  const selectedProjectIndex = projects.findIndex(
    p => p.lowerCaseName === lowerCaseName,
  );
  let reorderedProjects = projects;
  if (selectedProjectIndex !== -1) {
    reorderedProjects = [
      projects[selectedProjectIndex],
      ...projects.slice(0, selectedProjectIndex),
      ...projects.slice(selectedProjectIndex + 1),
    ];
  }
  return projects.length > 1 ? (
    <Combobox<ProjectRow>
      editable={false}
      className="project-picker-dropdown"
      items={reorderedProjects.map(p => ({
        text: p.name,
        value: p,
        icon: getProjectIcon(p),
      }))}
      selectedValue={projects.find(p => p.lowerCaseName === lowerCaseName)}
      onChange={onChange}
    />
  ) : (
    <></>
  );
}
