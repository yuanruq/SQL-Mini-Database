/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationDesc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.PackageDoc;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.SeeTag;
import com.sun.javadoc.SourcePosition;
import com.sun.javadoc.Tag;

class HidingProgramElementDocWrapper extends HidingWrapper
                                     implements ProgramElementDoc {
    public HidingProgramElementDocWrapper(ProgramElementDoc progelemdoc,
                                          Map mapWrappers) {
        super(progelemdoc, mapWrappers);
    }

    private ProgramElementDoc _getProgramElementDoc() {
        return (ProgramElementDoc)getWrappedObject();
    }

    public ClassDoc containingClass() {
        return (ClassDoc)wrapOrHide(_getProgramElementDoc().containingClass());
    }

    public PackageDoc containingPackage() {
        return (PackageDoc)
                wrapOrHide(_getProgramElementDoc().containingPackage());
    }

    public boolean isFinal() {
        return _getProgramElementDoc().isFinal();
    }

    public boolean isPackagePrivate() {
        return _getProgramElementDoc().isPackagePrivate();
    }

    public boolean isPrivate() {
        return _getProgramElementDoc().isPrivate();
    }

    public boolean isProtected() {
        return _getProgramElementDoc().isProtected();
    }

    public boolean isPublic() {
        return _getProgramElementDoc().isPublic();
    }

    public boolean isStatic() {
        return _getProgramElementDoc().isStatic();
    }

    public int modifierSpecifier() {
        return _getProgramElementDoc().modifierSpecifier();
    }

    public String modifiers() {
        return _getProgramElementDoc().modifiers();
    }

    public String qualifiedName() {
        return _getProgramElementDoc().qualifiedName();
    }

    public AnnotationDesc[] annotations() {
        return (AnnotationDesc[])
                wrapOrHide(_getProgramElementDoc().annotations()); 
    }

    public boolean isEnum() {
        return _getProgramElementDoc().isEnum();
    }

    public boolean isEnumConstant() {
        return _getProgramElementDoc().isEnumConstant();
    }
    
    public String commentText() {
        return _getProgramElementDoc().commentText();
    }

    public int compareTo(Object obj) {
        if (obj instanceof HidingWrapper) {
            return _getProgramElementDoc().
                   compareTo(((HidingWrapper)obj).getWrappedObject());
        } else { 
            return _getProgramElementDoc().compareTo(obj);
        }
    }

    public Tag[] firstSentenceTags() {
        return (Tag[])wrapOrHide(_getProgramElementDoc().firstSentenceTags());
    }

    public String getRawCommentText() {
        return _getProgramElementDoc().getRawCommentText();
    }

    public Tag[] inlineTags() {
        return (Tag[])wrapOrHide(_getProgramElementDoc().inlineTags());
    }

    public boolean isClass() {
        return _getProgramElementDoc().isClass();
    }

    public boolean isConstructor() {
        return _getProgramElementDoc().isConstructor();
    }

    public boolean isError() {
        return _getProgramElementDoc().isError();
    }

    public boolean isException() {
        return _getProgramElementDoc().isException();
    }

    public boolean isField() {
        return _getProgramElementDoc().isField();
    }

    public boolean isIncluded() {
        return _getProgramElementDoc().isIncluded();
    }

    public boolean isInterface() {
        return _getProgramElementDoc().isInterface();
    }

    public boolean isMethod() {
        return _getProgramElementDoc().isMethod();
    }

    public boolean isOrdinaryClass() {
        return _getProgramElementDoc().isOrdinaryClass();
    }

    public String name() {
        return _getProgramElementDoc().name();
    }

    public SeeTag[] seeTags() {
        return (SeeTag[])wrapOrHide(_getProgramElementDoc().seeTags());
    }

    public void setRawCommentText(String szText) {
        _getProgramElementDoc().setRawCommentText(szText);
    }

    public Tag[] tags() {
        return (Tag[])wrapOrHide(_getProgramElementDoc().tags());
    }

    public Tag[] tags(String szTagName) {
        return (Tag[])wrapOrHide(_getProgramElementDoc().tags(szTagName));
    }
    
    public SourcePosition position() {
        return (SourcePosition)wrapOrHide(_getProgramElementDoc().position());  
    }

    public boolean isAnnotationType() {
        return _getProgramElementDoc().isAnnotationType();
    }

    public boolean isAnnotationTypeElement() {
        return _getProgramElementDoc().isAnnotationTypeElement();
    }
}
